use crate::types::v0::{
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecTransaction,
    },
    transport::{NodeId, VolumeId},
};
use chrono::{DateTime, Utc};
use pstor::ApiVersion;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

/// Defines operation for SwitchOverSpec.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    /// Initialize switchover request.
    Init,
    /// Shutdown original/old volume target. Create new nexus for existing vol obj.
    RepublishVolume,
    /// Send updated path of volume to node-agent.
    ReplacePath,
    /// Delete original/old volume target.
    DeleteTarget,
    /// Marks switchover process as Complete.
    Successful,
    /// Represent failed switchover request.
    Errored(String),
}

/// Represent the state for the operation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperationState {
    operation: Operation,
    result: Option<bool>,
}

impl OperationState {
    /// Create a new `OperationState`.
    pub fn new(operation: Operation, result: Option<bool>) -> Self {
        Self { operation, result }
    }
}

/// Defines timestamp for the `SwitchOverSpec`.
pub type SwitchOverTime = DateTime<Utc>;

/// Represent switchover spec.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SwitchOverSpec {
    /// Uri of node-agent to report new path.
    pub callback_uri: SocketAddr,
    /// The nodename of the node-agent node.
    pub node_name: NodeId,
    /// Volume for which switchover needs to be executed.
    pub volume: VolumeId,
    /// Operation represent current running operation on SwitchOverSpec.
    pub operation: Option<OperationState>,
    /// Timestamp when switchover request was generated.
    pub timestamp: SwitchOverTime,
    /// Failed nexus path.
    pub existing_nqn: String,
    /// New nexus path.
    pub new_path: Option<String>,
    /// Number of failed attempts in the current Stage.
    pub retry_count: u64,
    /// Reuse the existing target.
    pub reuse_existing: bool,
    /// Publish context of the volume.
    pub publish_context: Option<HashMap<String, String>>,
}

impl SwitchOverSpec {
    /// Update spec with error message.
    pub fn set_error_msg(&mut self, msg: String) {
        self.operation = Some(OperationState {
            operation: Operation::Errored(msg),
            result: Some(false),
        })
    }

    /// If `Self` is marked as completed or not.
    pub fn is_completed(&self) -> bool {
        if let Some(op) = &self.operation {
            match op.operation {
                Operation::Errored(_) => true,
                Operation::Successful => op.result.unwrap_or(false),
                _ => false,
            }
        } else {
            false
        }
    }

    /// If relevant request was errored or not.
    pub fn is_errored(&self) -> bool {
        if let Some(op) = &self.operation {
            matches!(op.operation, Operation::Errored(_))
        } else {
            false
        }
    }

    /// Returns current `Operation` for `SwitchOverSpec`.
    pub fn operation(&self) -> Option<Operation> {
        self.operation.as_ref().map(|op| op.operation.clone())
    }
}

/// Persistent Store key for `SwitchOverSpec`.
pub struct SwitchOverSpecKey(VolumeId);

impl StorableObject for SwitchOverSpec {
    type Key = SwitchOverSpecKey;

    fn key(&self) -> Self::Key {
        SwitchOverSpecKey(self.volume.clone())
    }
}

impl SwitchOverSpecKey {
    pub fn new(id: VolumeId) -> Self {
        SwitchOverSpecKey(id)
    }
}

impl ObjectKey for SwitchOverSpecKey {
    type Kind = StorableObjectType;

    fn version(&self) -> ApiVersion {
        ApiVersion::V0
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::SwitchOver
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl SpecTransaction<Operation> for SwitchOverSpec {
    fn has_pending_op(&self) -> bool {
        self.operation.is_some()
    }

    fn commit_op(&mut self) {
        let next_op = if let Some(op) = self.operation.clone() {
            match op.operation {
                Operation::Init => Some(Operation::RepublishVolume),
                Operation::RepublishVolume => Some(Operation::ReplacePath),
                Operation::ReplacePath => Some(Operation::DeleteTarget),
                Operation::DeleteTarget => Some(Operation::Successful),
                Operation::Successful => None,
                Operation::Errored(_) => None,
            }
        } else {
            None
        };

        if let Some(op) = next_op {
            self.start_op(op);
        }
    }

    fn clear_op(&mut self) {
        self.operation = None;
    }

    fn start_op(&mut self, operation: Operation) {
        self.operation = Some(OperationState {
            operation,
            result: None,
        })
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.operation {
            op.result = Some(result);
        }
    }

    fn pending_op(&self) -> Option<&Operation> {
        self.operation.as_ref().map(|o| &o.operation)
    }
}
