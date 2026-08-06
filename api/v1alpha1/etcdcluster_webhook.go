/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/coreos/go-semver/semver"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	etcdversions "go.etcd.io/etcd/api/v3/version"
)

// etcdclusterlog is the logger used by the EtcdCluster webhooks. Every admission
// decision is logged with enough structured context (name, namespace, the
// offending field, and the user-facing remediation) that an operator reading the
// webhook-server logs can reconstruct exactly why a request was accepted,
// defaulted, or rejected.
var etcdclusterlog = logf.Log.WithName("etcdcluster-webhook")

// Known TLS providers. Kept in one place so the validator's accepted-set and the
// defaulter agree, and so the error message can enumerate the valid choices.
const (
	tlsProviderAuto        = "auto"
	tlsProviderCertManager = "cert-manager"
)

// knownTLSProviders is the canonical, lower-cased set of providers the operator
// understands, in a deterministic order for stable error messages.
var knownTLSProviders = []string{tlsProviderAuto, tlsProviderCertManager}

// SetupWebhookWithManager registers the validating and defaulting webhooks for
// EtcdCluster with the manager's webhook server. This is the single entry point
// called from cmd/main.go; previously the webhook server was started but nothing
// was ever registered with it (see issue #380).
func (r *EtcdCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	etcdclusterlog.Info("registering EtcdCluster admission webhooks (validating + defaulting)")
	return ctrl.NewWebhookManagedBy(mgr, r).
		WithCustomValidator(&EtcdClusterCustomValidator{}).
		WithCustomDefaulter(&EtcdClusterCustomDefaulter{}).
		Complete()
}

// Webhook policy rationale (applies to BOTH webhooks declared in this file).
//
//   - failurePolicy=fail: these webhooks are correctness-critical. Defaulting
//     (provider=auto) and validation (odd size, semver, supported upgrade path,
//     storageSpec immutability) protect a stateful, quorum-based datastore where a
//     bad spec admitted while the webhook is unreachable can produce a cluster that
//     cannot form quorum or silent data loss on a botched PVC change. We therefore
//     fail closed: if the API server cannot reach the webhook, the EtcdCluster
//     create/update is rejected rather than admitted unchecked.
//   - Failing closed does NOT brick the cluster. The rule scope is EXACTLY
//     {operator.etcd.io/v1alpha1 etcdclusters}, so a webhook outage only blocks
//     mutations to EtcdCluster CRs (a narrow, operator-owned resource); core
//     workloads, nodes, and every other API object stay fully editable. As an
//     explicit break-glass escape hatch, config/default injects an objectSelector
//     so an admin can label a single CR with
//     `etcd.operator.etcd.io/skip-webhooks: "true"` to bypass admission while the
//     webhook is down (see config/default/webhook_selector_patch.yaml).
//   - timeoutSeconds=10: validation is pure, in-memory, and dependency-free (no
//     external calls), so 10s is generous headroom that still bounds API-server
//     stalls if the webhook pod is wedged. With failurePolicy=fail a timeout is a
//     rejection, which is the safe direction.
//   - matchPolicy=Equivalent: match the request even if it arrives via a different
//     but equivalent API group/version, so a future served/stored version cannot
//     silently slip past the validator. Only v1alpha1 exists today, but Equivalent
//     is the safe default and costs nothing now.

// +kubebuilder:webhook:path=/mutate-operator-etcd-io-v1alpha1-etcdcluster,mutating=true,failurePolicy=fail,matchPolicy=Equivalent,timeoutSeconds=10,sideEffects=None,groups=operator.etcd.io,resources=etcdclusters,verbs=create;update,versions=v1alpha1,name=metcdcluster-v1alpha1.kb.io,admissionReviewVersions=v1

// EtcdClusterCustomDefaulter applies defaults to EtcdCluster objects on admission.
// +kubebuilder:object:generate=false
type EtcdClusterCustomDefaulter struct{}

var _ admission.Defaulter[runtime.Object] = &EtcdClusterCustomDefaulter{}

// Default implements webhook.CustomDefaulter.
func (d *EtcdClusterCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	ec, ok := obj.(*EtcdCluster)
	if !ok {
		return fmt.Errorf("expected an EtcdCluster object but got %T", obj)
	}
	log := etcdclusterlog.WithValues("name", ec.Name, "namespace", ec.Namespace)
	applyEtcdClusterDefaults(ec, log)
	return nil
}

// applyEtcdClusterDefaults mutates ec in place to fill in sensible defaults. It is
// pure (aside from the supplied logger) so it can be unit tested directly.
func applyEtcdClusterDefaults(ec *EtcdCluster, log logr) {
	// When a TLS block is present but no provider is named, the operator falls
	// back to the "auto" provider (matches the comment on TLSCertificate.Provider).
	// Materializing it here means downstream code and validation see an explicit,
	// canonical value instead of "".
	if ec.Spec.TLS != nil && strings.TrimSpace(ec.Spec.TLS.Provider) == "" {
		ec.Spec.TLS.Provider = tlsProviderAuto
		if log != nil {
			log.Info("defaulting spec.tls.provider to \"auto\" (no provider specified)")
		}
	}
}

// See the "Webhook policy rationale" comment above the mutating webhook marker;
// failurePolicy=fail, matchPolicy=Equivalent, and timeoutSeconds=10 are chosen on
// the same grounds for the validating webhook.

// +kubebuilder:webhook:path=/validate-operator-etcd-io-v1alpha1-etcdcluster,mutating=false,failurePolicy=fail,matchPolicy=Equivalent,timeoutSeconds=10,sideEffects=None,groups=operator.etcd.io,resources=etcdclusters,verbs=create;update,versions=v1alpha1,name=vetcdcluster-v1alpha1.kb.io,admissionReviewVersions=v1

// EtcdClusterCustomValidator validates EtcdCluster objects on admission.
// +kubebuilder:object:generate=false
type EtcdClusterCustomValidator struct{}

var _ admission.Validator[runtime.Object] = &EtcdClusterCustomValidator{}

// etcdClusterGroupKind is used when building aggregated field errors into an
// apierrors.NewInvalid status so the API server returns a structured 422.
var etcdClusterGroupKind = schema.GroupKind{Group: GroupVersion.Group, Kind: "EtcdCluster"}

// ValidateCreate implements webhook.CustomValidator.
func (v *EtcdClusterCustomValidator) ValidateCreate(
	_ context.Context, obj runtime.Object,
) (admission.Warnings, error) {
	ec, ok := obj.(*EtcdCluster)
	if !ok {
		return nil, fmt.Errorf("expected an EtcdCluster object but got %T", obj)
	}
	log := etcdclusterlog.WithValues("name", ec.Name, "namespace", ec.Namespace, "operation", "create")
	log.Info("validating EtcdCluster on create")

	errs := validateEtcdClusterSpec(&ec.Spec)
	return finish(log, ec, errs)
}

// ValidateUpdate implements webhook.CustomValidator.
func (v *EtcdClusterCustomValidator) ValidateUpdate(
	_ context.Context, oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	ec, ok := newObj.(*EtcdCluster)
	if !ok {
		return nil, fmt.Errorf("expected an EtcdCluster object for the new object but got %T", newObj)
	}
	oldEc, ok := oldObj.(*EtcdCluster)
	if !ok {
		return nil, fmt.Errorf("expected an EtcdCluster object for the old object but got %T", oldObj)
	}
	log := etcdclusterlog.WithValues("name", ec.Name, "namespace", ec.Namespace, "operation", "update")
	log.Info("validating EtcdCluster on update")

	errs := validateEtcdClusterUpdate(&oldEc.Spec, &ec.Spec)
	return finish(log, ec, errs)
}

// ValidateDelete implements webhook.CustomValidator. Deletion is always allowed.
func (v *EtcdClusterCustomValidator) ValidateDelete(
	_ context.Context, obj runtime.Object,
) (admission.Warnings, error) {
	if _, ok := obj.(*EtcdCluster); !ok {
		return nil, fmt.Errorf("expected an EtcdCluster object but got %T", obj)
	}
	return nil, nil
}

// finish converts an accumulated field.ErrorList into either nil (accept) or a
// structured apierrors.NewInvalid (reject), logging the outcome either way.
func finish(log logr, ec *EtcdCluster, errs field.ErrorList) (admission.Warnings, error) {
	if len(errs) == 0 {
		log.Info("EtcdCluster admitted")
		return nil, nil
	}
	// Log each rejection reason individually so the actionable detail is greppable.
	for _, e := range errs {
		log.Info("EtcdCluster rejected", "field", e.Field, "reason", e.Detail)
	}
	return nil, apierrors.NewInvalid(etcdClusterGroupKind, ec.Name, errs)
}

// ---------------------------------------------------------------------------
// Pure validation helpers. These take only the spec(s) and return field errors
// with crisp, actionable Detail strings. They are unit-tested directly, including
// exact message text.
// ---------------------------------------------------------------------------

// validateEtcdClusterSpec runs all create-time spec invariants. On update only the
// subset of these that changed is re-checked; see validateEtcdClusterUpdate.
func validateEtcdClusterSpec(spec *EtcdClusterSpec) field.ErrorList {
	errs := make(field.ErrorList, 0, 3)
	specPath := field.NewPath("spec")

	errs = append(errs, validateSize(spec.Size, specPath.Child("size"))...)
	errs = append(errs, validateVersionFormat(spec.Version, specPath.Child("version"))...)
	errs = append(errs, validateTLS(spec.TLS, specPath.Child("tls"))...)
	errs = append(errs, validateStorageSpec(spec.StorageSpec, specPath.Child("storageSpec"))...)

	return errs
}

// minVolumeSizeRequest mirrors the controller's reconcile-time floor
// (internal/controller/utils.go) so the webhook rejects an undersized PVC
// synchronously at apply time instead of letting the StatefulSet build fail later
// with no actionable feedback.
var minVolumeSizeRequest = resource.MustParse("1Mi")

// validateStorageSpec enforces the same storage invariants the controller checks
// while building the StatefulSet, but at admission time so the user gets an
// immediate, actionable rejection rather than a silently failing reconcile:
//
//   - volumeSizeRequest is required and must be >= 1Mi.
//   - volumeSizeLimit, if set, must be >= volumeSizeRequest (a PVC whose limit is
//     below its request is rejected by the API server when the StatefulSet is
//     created).
//   - accessModes must be one the operator handles (ReadWriteOnce, ReadWriteMany,
//     or empty -> ReadWriteOnce); ReadOnlyMany and others are unsupported.
//   - pvcName is required when accessModes is ReadWriteMany (the operator mounts a
//     pre-provisioned shared PVC by name in that mode).
func validateStorageSpec(s *StorageSpec, path *field.Path) field.ErrorList {
	if s == nil {
		return nil
	}
	var errs field.ErrorList

	switch {
	case s.VolumeSizeRequest.IsZero():
		errs = append(errs, field.Required(path.Child("volumeSizeRequest"),
			"volumeSizeRequest is required when storageSpec is set; specify a persistent volume size such as \"1Gi\"."))
	case s.VolumeSizeRequest.Cmp(minVolumeSizeRequest) < 0:
		errs = append(errs, field.Invalid(path.Child("volumeSizeRequest"), s.VolumeSizeRequest.String(),
			fmt.Sprintf("volumeSizeRequest must be at least 1Mi; got %q. Request a larger persistent volume (e.g. \"1Gi\").",
				s.VolumeSizeRequest.String())))
	}

	// Only meaningful to compare the limit when both are usable values; if the
	// request itself is invalid the message above is the actionable one.
	if !s.VolumeSizeLimit.IsZero() && !s.VolumeSizeRequest.IsZero() &&
		s.VolumeSizeLimit.Cmp(s.VolumeSizeRequest) < 0 {
		errs = append(errs, field.Invalid(path.Child("volumeSizeLimit"), s.VolumeSizeLimit.String(),
			fmt.Sprintf("volumeSizeLimit (%q) must be greater than or equal to volumeSizeRequest (%q); "+
				"raise volumeSizeLimit or lower volumeSizeRequest.",
				s.VolumeSizeLimit.String(), s.VolumeSizeRequest.String())))
	}

	switch s.AccessModes {
	case "", corev1.ReadWriteOnce:
		// default / single-writer: nothing else required.
	case corev1.ReadWriteMany:
		if strings.TrimSpace(s.PVCName) == "" {
			errs = append(errs, field.Required(path.Child("pvcName"),
				"pvcName is required when accessModes is \"ReadWriteMany\"; set it to the name of the "+
					"pre-provisioned shared PersistentVolumeClaim the etcd pods should mount."))
		}
	default:
		errs = append(errs, field.NotSupported(path.Child("accessModes"),
			s.AccessModes, []string{string(corev1.ReadWriteOnce), string(corev1.ReadWriteMany)}))
	}

	return errs
}

// validateSize enforces size >= 1 and an odd member count. etcd forms a quorum of
// (n/2)+1; an even cluster has the same fault tolerance as the next-lower odd size
// while being strictly more likely to lose quorum, so even sizes are rejected.
func validateSize(size int, path *field.Path) field.ErrorList {
	if size < 1 {
		return field.ErrorList{field.Invalid(path, size,
			fmt.Sprintf("size must be at least 1; got %d. Set spec.size to a positive odd number (e.g. 1, 3, or 5).", size))}
	}
	if size%2 == 0 {
		return field.ErrorList{field.Invalid(path, size,
			fmt.Sprintf("size must be an odd number so the cluster can form a majority quorum; got %d. "+
				"An even-sized etcd cluster tolerates no more failures than the next-smaller odd size while being more "+
				"likely to lose quorum. Use %d or %d instead.", size, size-1, size+1))}
	}
	return nil
}

// normalizeEtcdVersion strips an optional leading "v" from an etcd version string
// so it can be parsed by the CoreOS go-semver library, which rejects the "v"
// prefix. The canonical etcd release images at the default registry
// (gcr.io/etcd-development/etcd) are published ONLY with v-prefixed tags (e.g.
// "v3.5.21" — see config/samples), so users legitimately write spec.version with
// or without the "v". Both forms must validate identically here, and the
// controller (internal/controller/utils.go: etcdImageTag) re-applies the "v" the
// registry expects when rendering the image, so a bare spec.version still pulls.
func normalizeEtcdVersion(version string) string {
	return strings.TrimPrefix(strings.TrimSpace(version), "v")
}

// validateVersionFormat ensures spec.version is non-empty and parses as semver.
// A leading "v" is accepted (and stripped before parsing) because the default
// etcd-development registry only publishes v-prefixed tags; rejecting "v3.6.1"
// would make the operator's own working samples unappliable.
func validateVersionFormat(version string, path *field.Path) field.ErrorList {
	if strings.TrimSpace(version) == "" {
		return field.ErrorList{field.Required(path,
			"version is required; set spec.version to a semver etcd image tag such as \"3.6.1\".")}
	}
	if _, err := semver.NewVersion(normalizeEtcdVersion(version)); err != nil {
		return field.ErrorList{field.Invalid(path, version,
			fmt.Sprintf("version %q is not a valid semantic version (expected MAJOR.MINOR.PATCH, e.g. \"3.6.1\"): %v.",
				version, err))}
	}
	return nil
}

// validateTLS checks that the TLS surface is internally coherent: the provider is
// one the operator understands, and the provider-specific config block required by
// that provider is present and complete.
func validateTLS(tls *TLSCertificate, path *field.Path) field.ErrorList {
	if tls == nil {
		return nil
	}
	var errs field.ErrorList

	// An empty provider is defaulted to "auto" by the defaulting webhook; treat it
	// as "auto" here too so validation is correct even if the defaulter is bypassed
	// (e.g. in envtest where only the validating webhook is registered).
	provider := strings.TrimSpace(tls.Provider)
	if provider == "" {
		provider = tlsProviderAuto
	}

	cfgPath := path.Child("providerCfg")
	switch provider {
	case tlsProviderAuto:
		// auto provider self-generates certs; cert-manager config must not be set.
		if tls.ProviderCfg.CertManagerCfg != nil {
			errs = append(errs, field.Invalid(cfgPath.Child("certManagerCfg"),
				"<set>",
				"providerCfg.certManagerCfg must not be set when provider is \"auto\"; "+
					"either remove providerCfg.certManagerCfg or set provider to \"cert-manager\"."))
		}
		if ac := tls.ProviderCfg.AutoCfg; ac != nil {
			errs = append(errs, validateCommonName(ac.CommonName,
				cfgPath.Child("autoCfg").Child("commonName"))...)
		}
	case tlsProviderCertManager:
		cm := tls.ProviderCfg.CertManagerCfg
		if cm == nil {
			errs = append(errs, field.Required(cfgPath.Child("certManagerCfg"),
				"providerCfg.certManagerCfg is required when provider is \"cert-manager\"; "+
					"supply issuerKind and issuerName."))
			break
		}
		if strings.TrimSpace(cm.IssuerName) == "" {
			errs = append(errs, field.Required(cfgPath.Child("certManagerCfg").Child("issuerName"),
				"issuerName is required for the cert-manager provider; set it to the name of an Issuer or ClusterIssuer."))
		}
		if k := strings.TrimSpace(cm.IssuerKind); k != "" && k != "Issuer" && k != "ClusterIssuer" {
			errs = append(errs, field.NotSupported(
				cfgPath.Child("certManagerCfg").Child("issuerKind"),
				cm.IssuerKind, []string{"Issuer", "ClusterIssuer"}))
		}
		errs = append(errs, validateCommonName(cm.CommonName,
			cfgPath.Child("certManagerCfg").Child("commonName"))...)
	default:
		errs = append(errs, field.NotSupported(path.Child("provider"), tls.Provider, knownTLSProviders))
	}

	return errs
}

// maxCommonNameLen is the X.509 CommonName ceiling documented on CommonConfig: a
// CN longer than 64 characters produces an invalid CSR, so the cert never issues.
// Catching it at admission turns a silent never-ready certificate into an
// immediate, actionable rejection.
const maxCommonNameLen = 64

// validateCommonName enforces the documented 64-character CommonName limit. An
// empty CommonName is valid (the provider derives a default).
func validateCommonName(cn string, path *field.Path) field.ErrorList {
	if len(cn) <= maxCommonNameLen {
		return nil
	}
	return field.ErrorList{field.Invalid(path, cn,
		fmt.Sprintf("commonName must be %d characters or fewer to produce a valid X.509 CSR; got %d. "+
			"Shorten commonName (the certificate provider derives a default when it is empty).",
			maxCommonNameLen, len(cn)))}
}

// validateEtcdClusterUpdate runs all update-time invariants between the old and
// new spec. Unlike ValidateCreate it does NOT re-run the full create-time spec
// validation: the CRD historically only enforced "size >= 1", so pre-existing
// clusters with an even size or an unknown-but-well-formed version are legal on
// the API server. With failurePolicy: Fail and no objectSelector, re-validating
// those invariants on every update would wedge unrelated edits (metadata,
// annotations, TLS) to legacy clusters. We therefore only enforce the size and
// version-format invariants when the offending field actually changed — keeping
// the guardrail for new values while letting legacy clusters be remediated. TLS
// coherence is cheap and self-contained, so it is always re-validated.
func validateEtcdClusterUpdate(oldSpec, newSpec *EtcdClusterSpec) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if newSpec.Size != oldSpec.Size {
		errs = append(errs, validateSize(newSpec.Size, specPath.Child("size"))...)
	}
	if newSpec.Version != oldSpec.Version {
		errs = append(errs, validateVersionFormat(newSpec.Version, specPath.Child("version"))...)
	}
	errs = append(errs, validateTLS(newSpec.TLS, specPath.Child("tls"))...)

	// storageSpec is immutable once set: changing the PVC template after the
	// StatefulSet exists cannot be reconciled in place and risks data loss.
	switch {
	case oldSpec.StorageSpec != nil && newSpec.StorageSpec == nil:
		errs = append(errs, field.Invalid(specPath.Child("storageSpec"), nil,
			"storageSpec is immutable and cannot be removed once set; restore the original storageSpec or recreate the cluster."))
	case oldSpec.StorageSpec != nil && newSpec.StorageSpec != nil &&
		!apiequality.Semantic.DeepEqual(oldSpec.StorageSpec, newSpec.StorageSpec):
		errs = append(errs, field.Invalid(specPath.Child("storageSpec"), newSpec.StorageSpec,
			"storageSpec is immutable and cannot be changed once set; revert spec.storageSpec to its original value or recreate the cluster."))
	case oldSpec.StorageSpec == nil && newSpec.StorageSpec != nil:
		// Adding storage to a cluster that previously had none is permitted (it is
		// not yet "set"), but the new block must itself be valid — otherwise the
		// content invariants would only be enforced at create time and a malformed
		// storageSpec could be introduced via update.
		errs = append(errs, validateStorageSpec(newSpec.StorageSpec, specPath.Child("storageSpec"))...)
	}

	// Upgrade-path check. Only meaningful when both versions parse and actually
	// differ; format errors are already surfaced by validateVersionFormat.
	errs = append(errs, validateUpgradePath(oldSpec.Version, newSpec.Version, specPath.Child("version"))...)

	return errs
}

// validateUpgradePath rejects unsupported version transitions (skip-minor
// upgrades and downgrades), reusing the same ordered version table the controller
// uses at reconcile time. A no-op (equal versions) or an unparseable version is
// not reported here (the latter is handled by validateVersionFormat).
//
// Note: this validates the *declared* spec transition (old spec.version ->
// new spec.version), not the actual running version. The controller compares the
// live StatefulSet image against the spec at reconcile time and remains
// authoritative; if a prior upgrade stalled before convergence, a skip-minor edit
// could pass this declared-transition check yet still be caught by the controller.
func validateUpgradePath(current, target string, path *field.Path) field.ErrorList {
	// Compare on the normalized form so "v3.6.1" -> "3.6.1" (same release, just a
	// prefix difference) is correctly treated as a no-op rather than a transition.
	if normalizeEtcdVersion(current) == normalizeEtcdVersion(target) {
		return nil
	}
	if _, err := semver.NewVersion(normalizeEtcdVersion(current)); err != nil {
		return nil // old object had a bad version; nothing actionable to add here.
	}
	if _, err := semver.NewVersion(normalizeEtcdVersion(target)); err != nil {
		return nil // format error already reported by validateVersionFormat.
	}

	if err := checkUpgradePath(etcdversions.AllVersions, current, target); err != nil {
		return field.ErrorList{field.Invalid(path, target,
			fmt.Sprintf("%v. etcd only supports sequential single-minor upgrades and forbids downgrades; "+
				"upgrade one minor version at a time (current %q -> target %q).", err, current, target))}
	}
	return nil
}

// checkUpgradePath mirrors the controller's validateEtcdUpgradePath ordering logic
// but is local to the api package (controller internals are not importable here)
// and returns only the actionable error. supportedVersions must be ascending.
func checkUpgradePath(supportedVersions []semver.Version, current, target string) error {
	// go-semver rejects a leading "v"; normalize so a v-prefixed spec.version (the
	// default registry's tag convention) participates in the ordering check.
	currentVer, err := semver.NewVersion(normalizeEtcdVersion(current))
	if err != nil {
		return fmt.Errorf("failed to parse current version %s: %w", current, err)
	}
	targetVer, err := semver.NewVersion(normalizeEtcdVersion(target))
	if err != nil {
		return fmt.Errorf("failed to parse target version %s: %w", target, err)
	}

	currentIdx, targetIdx := -1, -1
	for idx, v := range supportedVersions {
		if v.Major == currentVer.Major && v.Minor == currentVer.Minor {
			currentIdx = idx
		}
		if v.Major == targetVer.Major && v.Minor == targetVer.Minor {
			targetIdx = idx
		}
	}

	switch {
	case currentIdx == -1:
		return fmt.Errorf("unknown current version %s (supported minor lines: %s)",
			currentVer, supportedMinorLines(supportedVersions))
	case targetIdx == -1:
		return fmt.Errorf("unknown target version %s (supported minor lines: %s)",
			targetVer, supportedMinorLines(supportedVersions))
	case currentIdx > targetIdx || (currentIdx == targetIdx && currentVer.Patch > targetVer.Patch):
		return fmt.Errorf("downgrading from version %s to version %s is not allowed", currentVer, targetVer)
	case targetIdx > currentIdx+1:
		return fmt.Errorf("upgrading from version %s to version %s is not allowed (skips a minor version)",
			currentVer, targetVer)
	}
	return nil
}

// supportedMinorLines renders the supported MAJOR.MINOR lines for error messages.
func supportedMinorLines(versions []semver.Version) string {
	seen := make(map[string]struct{}, len(versions))
	var lines []string
	for _, v := range versions {
		key := fmt.Sprintf("%d.%d", v.Major, v.Minor)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		lines = append(lines, key)
	}
	sort.Strings(lines)
	return strings.Join(lines, ", ")
}

// logr is the minimal logging surface applyEtcdClusterDefaults needs; it lets the
// unit tests pass nil while production code passes a real logr.Logger.
type logr interface {
	Info(msg string, keysAndValues ...any)
}
