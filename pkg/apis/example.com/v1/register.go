package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the group version used to register these objects.
// It defines the API Group and Version for StatusWatch resources.
var SchemeGroupVersion = schema.GroupVersion{Group: "statuswatch.example.com", Version: "v1"}

// SchemeBuilder is a global instance used to register the Go types of the CRDs
// with the Kubernetes scheme. This registration is necessary for the Kubernetes
// API to recognize and work with these custom types.
var (
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
)

// AddToScheme is a convenience function to simplify adding all the types in this
// group-version to a given scheme. When called, it registers the custom resource
// types with the Kubernetes API scheme.
var AddToScheme = SchemeBuilder.AddToScheme

// Note: Removed the redundant SchemeGroupVersion declaration since GroupVersion
// already serves the intended purpose.

// Kind returns a GroupKind for the specified kind in this group-version.
// This is useful for constructing specific references to the kind that include
// the API group information.
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource returns a GroupResource for the specified resource in this group-version.
// It's used to construct references to the resource that include the API group information,
// which is necessary for making API requests that are scoped to this custom resource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

func init() {
	// Registers the StatusWatch and StatusWatchList types with the Kubernetes
	// scheme. This step is crucial for the Kubernetes API server to recognize
	// and handle these custom resource definitions.
	localSchemeBuilder.Register(addKnownTypes)
}

// Adds the list of known types to the given scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&StatusWatch{},
		&StatusWatchList{},
	)

	scheme.AddKnownTypes(SchemeGroupVersion,
		&metav1.Status{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
