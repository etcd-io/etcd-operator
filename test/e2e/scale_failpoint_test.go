package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.etcd.io/etcd-operator/test/utils"
	gofail "go.etcd.io/gofail/runtime"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	operatorv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

var _ = Describe("Scale scenarios with failpoints", Ordered, Label("failpoint"), func() {

	var (
		controllerPodName string
		etcdClusterName   = "failpoint-test"
	)

	BeforeAll(func() {
		By("creating test namespace if not exists")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, _ = utils.Run(cmd) // Ignore the error (if there is one already)

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		// Wait for the Controller to become Running
		waitForControllerRunning(&controllerPodName)
	})

	AfterAll(func() {
		By("undeploying the controller-manager")
		cmd := exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace (if empty)")
		cmd = exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found=true")
		_, _ = utils.Run(cmd)
	})

	AfterEach(func() {
		// Processing when we want to keep logs, events, etc. in case of failure
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			dumpLogsAndEvents(controllerPodName)
		}
	})

	Context("Scale out with CrashAfterAddMember failpoint", func() {
		BeforeAll(func() {
			By("Enabling CrashAfterAddMember failpoint (inject panic)")
			// gofail: var CrashAfterAddMember struct{}
			err := gofail.Enable("CrashAfterAddMember",
				`panic("failpoint CrashAfterAddMember triggered")`)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterAll(func() {
			By("Disabling CrashAfterAddMember failpoint")
			_ = gofail.Disable("CrashAfterAddMember")
		})

		It("should eventually scale out to size=2 even if operator crashes in the middle", func() {
			By("Creating an EtcdCluster with size=1")
			createOrUpdateEtcdCluster(etcdClusterName, 1)

			By("Waiting until the cluster is stable at size=1")
			waitForEtcdClusterReady(etcdClusterName, 1)

			By("Patching the EtcdCluster to size=2 to trigger scale-out")
			patchEtcdClusterSize(etcdClusterName, 2)

			By("Expecting operator to panic on CrashAfterAddMember -> operator restarts")
			Eventually(func(g Gomega) {
				// To detect a crash, the Pod will temporarily go into CrashLoopBackOff and check the restart count.
				out, err := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.containerStatuses[0].restartCount}").CombinedOutput()
				g.Expect(err).NotTo(HaveOccurred())
				restartCount := string(out)
				// If the program has been restarted at least once, it is considered to have panicked.
				g.Expect(restartCount).NotTo(Equal("0"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed(), "operator did not restart")

			By("Waiting for operator to become Running again")
			waitForControllerRunning(&controllerPodName)

			By("Eventually the cluster should scale out to 2 replicas")
			waitForEtcdClusterReady(etcdClusterName, 2)
		})
	})

	Context("Scale in with CrashAfterRemoveMember failpoint", func() {
		BeforeAll(func() {
			By("Enabling CrashAfterRemoveMember failpoint (inject panic)")
			// gofail: var CrashAfterRemoveMember struct{}
			err := gofail.Enable("CrashAfterRemoveMember",
				`panic("failpoint CrashAfterRemoveMember triggered")`)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterAll(func() {
			By("Disabling CrashAfterRemoveMember failpoint")
			_ = gofail.Disable("CrashAfterRemoveMember")
		})

		It("should eventually scale in to size=1 even if operator crashes in the middle", func() {
			By("Creating an EtcdCluster with size=2")
			createOrUpdateEtcdCluster(etcdClusterName, 2)

			By("Waiting until the cluster is stable at size=2")
			waitForEtcdClusterReady(etcdClusterName, 2)

			By("Patching the EtcdCluster to size=1 to trigger scale-in")
			patchEtcdClusterSize(etcdClusterName, 1)

			By("Expecting operator to panic on CrashAfterRemoveMember -> operator restarts")
			Eventually(func(g Gomega) {
				out, err := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.containerStatuses[0].restartCount}").CombinedOutput()
				g.Expect(err).NotTo(HaveOccurred())
				restartCount := string(out)
				g.Expect(restartCount).NotTo(Equal("0"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed(), "operator did not restart on remove")

			By("Waiting for operator to become Running again")
			waitForControllerRunning(&controllerPodName)

			By("Eventually the cluster should scale in to 1 replica")
			waitForEtcdClusterReady(etcdClusterName, 1)
		})
	})

})

// createOrUpdateEtcdCluster creates or updates an EtcdCluster resource with the specified size.
func createOrUpdateEtcdCluster(name string, size int) {
	cluster := &operatorv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: operatorv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: "3.5.17",
		},
	}
	b, _ := json.Marshal(cluster)
	// Simple with kubectl apply
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(string(b))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to create/update EtcdCluster")
}

// patchEtcdClusterSize patches the EtcdCluster's spec.size
func patchEtcdClusterSize(name string, newSize int) {
	patchStr := fmt.Sprintf(`{"spec":{"size":%d}}`, newSize)
	cmd := exec.Command("kubectl", "-n", namespace, "patch", "etcdclusters", name,
		"--type=merge", "-p", patchStr)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to patch EtcdCluster size")
}

// waitForEtcdClusterReady waits until the statefulset's readyReplicas == desired.
func waitForEtcdClusterReady(name string, desired int) {
	Eventually(func(g Gomega) {
		sts := &appsv1.StatefulSet{}
		cmd := exec.Command("kubectl", "get", "statefulset", name, "-n", namespace, "-o", "json")
		out, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())

		err = json.Unmarshal([]byte(out), sts)
		g.Expect(err).NotTo(HaveOccurred(), "failed to unmarshal statefulset")

		g.Expect(sts.Status.ReadyReplicas).To(BeEquivalentTo(desired),
			fmt.Sprintf("waiting for ReadyReplicas=%d, got %d", desired, sts.Status.ReadyReplicas))
	}, 5*time.Minute, 5*time.Second).Should(Succeed())
}

// waitForControllerRunning waits until the controller-manager Pod is Running.
func waitForControllerRunning(controllerPodName *string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get",
			"pods", "-l", "control-plane=controller-manager",
			"-o", "jsonpath={.items[0].metadata.name}",
			"-n", namespace,
		)
		podOutput, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod name")

		*controllerPodName = strings.TrimSpace(podOutput)
		g.Expect(*controllerPodName).NotTo(BeEmpty(), "no controller-manager pod found")

		cmd = exec.Command("kubectl", "get",
			"pods", *controllerPodName, "-o", "jsonpath={.status.phase}",
			"-n", namespace,
		)
		phaseOutput, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(phaseOutput).To(Equal("Running"), "controller-manager pod is not running yet")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// dumpLogsAndEvents is called on test failure to print logs, events, etc.
func dumpLogsAndEvents(podName string) {
	By("Fetching controller manager pod logs")
	cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
	controllerLogs, err := utils.Run(cmd)
	if err == nil {
		fmt.Fprintf(GinkgoWriter, "\n--- controller-manager logs ---\n%s\n", controllerLogs)
	} else {
		fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s\n", err)
	}

	By("Fetching Kubernetes events in the namespace")
	cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
	eventsOutput, err := utils.Run(cmd)
	if err == nil {
		fmt.Fprintf(GinkgoWriter, "\n--- K8s Events ---\n%s\n", eventsOutput)
	} else {
		fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s\n", err)
	}
}
