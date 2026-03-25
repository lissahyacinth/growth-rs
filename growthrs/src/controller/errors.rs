use std::fmt;

use kube::runtime::controller::RunnerError;

use crate::offering::QuantityParseError;

/// Error from the kube controller event stream.
#[derive(Debug, thiserror::Error)]
pub enum ControllerStreamError {
    #[error("object {0} not found in local store")]
    ObjectNotFound(String),
    #[error("event queue error")]
    QueueError(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("controller runner error")]
    RunnerError(#[source] RunnerError),
}

impl From<ControllerStreamError> for ControllerError {
    fn from(value: ControllerStreamError) -> Self {
        ControllerError::Stream(value)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    QuantityParseError(#[from] QuantityParseError),
    #[error(transparent)]
    EnvConfigError(#[from] envconfig::Error),
    #[error("{0}")]
    ServerTypeUnavailableError(String),
    #[error("{0}")]
    Other(String),
}

/// Error type for controller failures.
#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error(transparent)]
    Kube(#[from] kube::Error),
    #[error("Error in Configuration - {0}")]
    ConfigError(#[from] ConfigError),
    #[error("{0} missing metadata.name — this indicates an issue in installed CRDs")]
    MissingName(&'static str),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("fault injection triggered after {0} NodeRequest creates")]
    FaultInjected(usize),
    #[error(transparent)]
    Stream(ControllerStreamError),
    #[error("{context}")]
    WithContext {
        context: &'static str,
        #[source]
        source: Box<ControllerError>,
    },
}

impl ControllerError {
    pub fn with_context(self, context: &'static str) -> Self {
        ControllerError::WithContext {
            context,
            source: Box::new(self),
        }
    }

    pub fn from_controller_error<W>(
        err: kube::runtime::controller::Error<ControllerError, W>,
    ) -> Self
    where
        W: std::error::Error + Send + Sync + 'static,
    {
        use kube::runtime::controller::Error as CE;
        match err {
            CE::ObjectNotFound(obj) => {
                ControllerStreamError::ObjectNotFound(fmt::format(format_args!("{obj}"))).into()
            }
            CE::ReconcilerFailed(source, _) => source,
            CE::QueueError(source) => ControllerStreamError::QueueError(Box::new(source)).into(),
            CE::RunnerError(source) => ControllerStreamError::RunnerError(source).into(),
        }
    }
}
