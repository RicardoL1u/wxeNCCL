package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// Database describes a database.

// StatusWatchSpec defines the desired state of StatusWatch
type StatusWatchSpec struct {
	Number  int      `json:"number,omitempty"`
	Workers []string `json:"workers,omitempty"`
}

// StatusWatchStatus defines the observed state of StatusWatch
type StatusWatchStatus struct {
	// 可以根据需要添加状态字段
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// StatusWatch is the Schema for the statuswatches API
type StatusWatch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StatusWatchSpec   `json:"spec,omitempty"`
	Status StatusWatchStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StatusWatchList contains a list of StatusWatch
type StatusWatchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StatusWatch `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StatusWatch{}, &StatusWatchList{})
}
