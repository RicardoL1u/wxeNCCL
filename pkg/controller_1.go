package controllers

import (
	"context"

	v1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusWatchReconciler reconciles a StatusWatch object
type StatusWatchReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile reconciles a StatusWatch object
func (r *StatusWatchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var statusWatch v1.StatusWatch
	if err := r.Get(ctx, req.NamespacedName, &statusWatch); err != nil {
		// handle not found (delete) and return appropriately
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// If everything went fine, return without requeuing
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatusWatchReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.StatusWatch{}).
		Complete(r)
}
