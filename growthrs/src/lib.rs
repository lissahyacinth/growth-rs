pub mod controller;
pub mod crds;
pub mod offering;
pub mod optimiser;
pub mod providers;

/// Shared test helpers for integration tests and the `test_pod` binary.
///
/// Always compiled — contains no heavy dependencies and the compiler
/// eliminates unused code in release builds.
pub mod testing;
