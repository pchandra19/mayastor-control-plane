use tokio::process::Command;
use tonic::Status;

pub(crate) mod bin;

const CSI_NODE_BINARY: &str = "csi-node";
const MOUNT: &str = "mount";
const UNMOUNT: &str = "unmount";
const SRC_PATH: &str = "--src-path";
const DSC_PATH: &str = "--dsc-path";
const TARGET_PATH: &str = "--target-path";
const MOUNT_FLAGS: &str = "--mount-flags";
const UNMOUNT_FLAGS: &str = "--unmount-flags";

pub async fn mount(src_path: &str, dsc_path: &str, mount_flags: &str) -> Result<(), Status> {
    let src_path = src_path.to_string();
    let dsc_path = dsc_path.to_string();
    let mount_flags = mount_flags.to_string();
    let binary_name = std::env::current_exe().unwrap_or(CSI_NODE_BINARY.into());
    tokio::spawn(async move {
        match Command::new(binary_name)
            .arg(MOUNT)
            .arg(SRC_PATH)
            .arg(&src_path)
            .arg(DSC_PATH)
            .arg(&dsc_path)
            .arg(MOUNT_FLAGS)
            .arg(&mount_flags)
            .output()
            .await
        {
            Ok(output) if output.status.success() => Ok(()),
            _ => Err(Status::aborted(format!(
                "Failed to execute mount for {}, {}, {}",
                src_path, dsc_path, mount_flags
            ))),
        }
    })
    .await
    .map_err(|e| Status::aborted(e.to_string()))?
}

pub async fn unmount(target_path: &str, unmount_flags: &str) -> Result<(), Status> {
    let target_path = target_path.to_string();
    let unmount_flags = unmount_flags.to_string();
    let binary_name = std::env::current_exe().unwrap_or(CSI_NODE_BINARY.into());
    tokio::spawn(async move {
        match Command::new(binary_name)
            .arg(MOUNT)
            .arg(TARGET_PATH)
            .arg(&target_path)
            .arg(UNMOUNT_FLAGS)
            .arg(&unmount_flags)
            .output()
            .await
        {
            Ok(output) if output.status.success() => Ok(()),
            _ => Err(Status::aborted(format!(
                "Failed to execute unmount for {}, {}",
                target_path, unmount_flags
            ))),
        }
    })
    .await
    .map_err(|e| Status::aborted(e.to_string()))?
}
