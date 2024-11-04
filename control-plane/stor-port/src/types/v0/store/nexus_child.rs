use crate::types::v0::{
    store::nexus::ReplicaUri,
    transport::{Child, ChildUri},
};

use serde::{Deserialize, Serialize};
use std::string::ToString;

/// Nexus children (replica or "raw" URI)
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum NexusChild {
    /// When the child is a pool replica (in case of a volume)
    Replica(ReplicaUri),
    /// When the child is just a "raw" URI (could be anything)
    Uri(ChildUri),
}

impl NexusChild {
    /// Return Self as ReplicaUri.
    pub fn as_replica(&self) -> Option<ReplicaUri> {
        match &self {
            NexusChild::Replica(replica) => Some(replica.clone()),
            NexusChild::Uri(_) => None,
        }
    }
    /// Return Self as ReplicaUri
    pub fn as_replica_ref(&self) -> Option<&ReplicaUri> {
        match &self {
            NexusChild::Replica(replica) => Some(replica),
            NexusChild::Uri(_) => None,
        }
    }
    /// Get the child URI
    pub fn uri(&self) -> ChildUri {
        match &self {
            NexusChild::Replica(replica) => replica.uri().clone(),
            NexusChild::Uri(uri) => uri.clone(),
        }
    }
}

impl std::fmt::Display for NexusChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NexusChild::Replica(replica) => replica.uri(),
                NexusChild::Uri(uri) => uri,
            }
        )
    }
}

impl From<NexusChild> for String {
    fn from(src: NexusChild) -> Self {
        src.to_string()
    }
}
impl From<&ReplicaUri> for NexusChild {
    fn from(src: &ReplicaUri) -> Self {
        NexusChild::Replica(src.clone())
    }
}
impl From<&ChildUri> for NexusChild {
    fn from(src: &ChildUri) -> Self {
        NexusChild::Uri(src.clone())
    }
}
impl From<ChildUri> for NexusChild {
    fn from(src: ChildUri) -> Self {
        NexusChild::Uri(src)
    }
}
impl From<NexusChild> for ChildUri {
    fn from(src: NexusChild) -> Self {
        src.uri()
    }
}
impl From<&str> for NexusChild {
    fn from(src: &str) -> Self {
        NexusChild::Uri(src.into())
    }
}
impl From<String> for NexusChild {
    fn from(src: String) -> Self {
        NexusChild::Uri(src.into())
    }
}
impl PartialEq<Child> for NexusChild {
    fn eq(&self, other: &Child) -> bool {
        self.uri() == other.uri
    }
}
