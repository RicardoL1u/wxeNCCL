package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:openapi-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// StatusWatch is the Schema for the statuswatches API
type StatusWatch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              StatusWatchSpec   `json:"spec,omitempty"`
	Status            StatusWatchStatus `json:"status,omitempty"`
}

type WorkerSpec struct {
	PodUUID string `json:"podUUID,omitempty"`
	Name    string `json:"name,omitempty"`
}

// StatusWatchSpec defines the desired state of StatusWatch
type StatusWatchSpec struct {
	Number    int          `json:"number,omitempty"`
	ServiceIP string       `json:"serviceIP,omitempty"`
	Workers   []WorkerSpec `json:"workers,omitempty"`
}

// StatusWatchStatus defines the observed state of StatusWatch
type StatusWatchStatus struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// StatusWatchList contains a list of StatusWatch
type StatusWatchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StatusWatch `json:"items"`
}
