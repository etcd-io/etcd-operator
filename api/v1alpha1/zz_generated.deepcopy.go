//go:build !ignore_autogenerated

/*
Copyright 2024.

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
	netx "net"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *EtcdCluster) DeepCopyInto(out *EtcdCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new EtcdCluster.
func (in *EtcdCluster) DeepCopy() *EtcdCluster {
	if in == nil {
		return nil
	}
	out := new(EtcdCluster)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *EtcdCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *EtcdClusterList) DeepCopyInto(out *EtcdClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]EtcdCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new EtcdClusterList.
func (in *EtcdClusterList) DeepCopy() *EtcdClusterList {
	if in == nil {
		return nil
	}
	out := new(EtcdClusterList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *EtcdClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *EtcdClusterSpec) DeepCopyInto(out *EtcdClusterSpec) {
	*out = *in
	if in.StorageSpec != nil {
		in, out := &in.StorageSpec, &out.StorageSpec
		*out = new(StorageSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.TLS != nil {
		in, out := &in.TLS, &out.TLS
		*out = new(TLSCertificate)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new EtcdClusterSpec.
func (in *EtcdClusterSpec) DeepCopy() *EtcdClusterSpec {
	if in == nil {
		return nil
	}
	out := new(EtcdClusterSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *EtcdClusterStatus) DeepCopyInto(out *EtcdClusterStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new EtcdClusterStatus.
func (in *EtcdClusterStatus) DeepCopy() *EtcdClusterStatus {
	if in == nil {
		return nil
	}
	out := new(EtcdClusterStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ProviderAutoConfig) DeepCopyInto(out *ProviderAutoConfig) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ProviderAutoConfig.
func (in *ProviderAutoConfig) DeepCopy() *ProviderAutoConfig {
	if in == nil {
		return nil
	}
	out := new(ProviderAutoConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ProviderCertManagerConfig) DeepCopyInto(out *ProviderCertManagerConfig) {
	*out = *in
	if in.DNSNames != nil {
		in, out := &in.DNSNames, &out.DNSNames
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.IPs != nil {
		in, out := &in.IPs, &out.IPs
		*out = make([]netx.IP, len(*in))
		for i := range *in {
			if (*in)[i] != nil {
				in, out := &(*in)[i], &(*out)[i]
				*out = make(netx.IP, len(*in))
				copy(*out, *in)
			}
		}
	}
	if in.Organization != nil {
		in, out := &in.Organization, &out.Organization
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ProviderCertManagerConfig.
func (in *ProviderCertManagerConfig) DeepCopy() *ProviderCertManagerConfig {
	if in == nil {
		return nil
	}
	out := new(ProviderCertManagerConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ProviderConfig) DeepCopyInto(out *ProviderConfig) {
	*out = *in
	if in.AutoCfg != nil {
		in, out := &in.AutoCfg, &out.AutoCfg
		*out = new(ProviderAutoConfig)
		**out = **in
	}
	if in.CertManagerCfg != nil {
		in, out := &in.CertManagerCfg, &out.CertManagerCfg
		*out = new(ProviderCertManagerConfig)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ProviderConfig.
func (in *ProviderConfig) DeepCopy() *ProviderConfig {
	if in == nil {
		return nil
	}
	out := new(ProviderConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *StorageSpec) DeepCopyInto(out *StorageSpec) {
	*out = *in
	out.VolumeSizeRequest = in.VolumeSizeRequest.DeepCopy()
	out.VolumeSizeLimit = in.VolumeSizeLimit.DeepCopy()
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new StorageSpec.
func (in *StorageSpec) DeepCopy() *StorageSpec {
	if in == nil {
		return nil
	}
	out := new(StorageSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TLSCertificate) DeepCopyInto(out *TLSCertificate) {
	*out = *in
	if in.ProviderCfg != nil {
		in, out := &in.ProviderCfg, &out.ProviderCfg
		*out = new(ProviderConfig)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TLSCertificate.
func (in *TLSCertificate) DeepCopy() *TLSCertificate {
	if in == nil {
		return nil
	}
	out := new(TLSCertificate)
	in.DeepCopyInto(out)
	return out
}
