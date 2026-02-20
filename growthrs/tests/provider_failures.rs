// Provider failure tests (creation_failed, internal_error, partial_progress)
// have been removed from here because reconcile_pods no longer calls
// provider.create() directly. These failure modes will be tested in the
// NodeRequest reconciler, which advances Pending → Provisioning via the provider.
