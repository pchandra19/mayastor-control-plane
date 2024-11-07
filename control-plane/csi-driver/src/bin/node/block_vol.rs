//! Functions for CSI publish and unpublish block mode volumes.

use std::path::Path;
use tonic::{Code, Status};
use tracing::{debug, error, info, warn};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use crate::{
    dev::Device,
    findmnt,
    mount::{self},
};
use csi_driver::csi::{NodePublishVolumeRequest, NodeUnpublishVolumeRequest};

pub(crate) async fn publish_block_volume(msg: &NodePublishVolumeRequest) -> Result<(), Status> {
    let target_path = &msg.target_path;
    let volume_id = &msg.volume_id;

    let uri = msg.publish_context.get("uri").ok_or_else(|| {
        failure!(
            Code::InvalidArgument,
            "Failed to stage volume {}: URI attribute missing from publish context",
            volume_id
        )
    })?;

    // Block volumes are not staged, instead
    // bind mount to the device path,
    // this can be done for mutliple target paths.
    let device = Device::parse(uri).map_err(|error| {
        failure!(
            Code::Internal,
            "Failed to publish volume {}: error parsing URI {}: {}",
            volume_id,
            uri,
            error
        )
    })?;

    if let Some(device_path) = device.find().await.map_err(|error| {
        failure!(
            Code::Internal,
            "Failed to publish volume {}: error locating device for URI {}: {}",
            volume_id,
            uri,
            error
        )
    })? {
        let path_target = Path::new(target_path);
        if path_target.exists() && !path_target.is_file() && !path_target.is_dir() {
            //target exists and is a special file

            // Idempotency, if we have done this already just return success.
            match findmnt::get_devicepath(target_path).await {
                Ok(findmnt_dev) => {
                    if let Some(fm_devpath) = findmnt_dev {
                        if fm_devpath == device_path {
                            debug!(
                                "{}({}) is already mounted onto {}",
                                fm_devpath, device_path, target_path
                            );
                            return Ok(());
                        } else {
                            return Err(Status::new(
                                Code::Internal,
                                format!(
                                    "Failed to publish volume {volume_id}: found device {fm_devpath} mounted at {target_path}, not {device_path}")));
                        }
                    }
                }
                Err(err) => {
                    return Err(Status::new(
                        Code::Internal,
                        format!(
                            "Failed to publish volume {volume_id}: error whilst checking mount on {target_path} : {err}"
                        ),
                    ));
                }
            }
        }

        if !path_target.exists() {
            std::fs::File::create(target_path.clone())?;
        }

        if let Err(error) =
            mount::blockdevice_mount(device_path, target_path.clone(), msg.readonly).await
        {
            return Err(failure!(
                Code::Internal,
                "Failed to publish volume {}: {}",
                volume_id,
                error
            ));
        }
        Ok(())
    } else {
        Err(failure!(
            Code::Internal,
            "Failed to publish volume {}: unable to retrieve device path for {}",
            volume_id,
            uri
        ))
    }
}

pub(crate) async fn unpublish_block_volume(msg: &NodeUnpublishVolumeRequest) -> Result<(), Status> {
    let target_path = msg.target_path.clone();
    let volume_id = msg.volume_id.clone();

    // block volumes are mounted on block special file, which is not
    // a regular file.
    if mount::find_mount(None, Some(target_path.clone()))
        .await
        .is_some()
    {
        match mount::blockdevice_unmount(target_path.clone()).await {
            Ok(_) => {}
            Err(err) => {
                return Err(Status::new(
                    Code::Internal,
                    format!("Failed to unpublish volume {volume_id}: {err}"),
                ));
            }
        }
    }

    debug!("Removing block special file {}", target_path);

    if let Err(error) = std::fs::remove_file(target_path.clone()) {
        warn!("Failed to remove block file {}: {}", target_path, error);
    }

    info!("Volume {} unpublished from {}", volume_id, target_path);
    Ok(())
}
