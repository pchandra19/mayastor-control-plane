//! Utility functions for mounting and unmounting filesystems.
use crate::filesystem_ops::FileSystem;
use csi_driver::filesystem::FileSystem as Fs;
use devinfo::mountinfo::{MountInfo, SafeMountIter};

use crate::runtime;
use std::{collections::HashSet, io::Error};
use sys_mount::{unmount, FilesystemType, Mount, MountFlags, UnmountFlags};
use tracing::{debug, error, info};
use uuid::Uuid;

// Simple trait for checking if the readonly (ro) option
// is present in a "list" of options, while allowing for
// flexibility as to the type of "list".
pub(super) trait ReadOnly {
    fn readonly(&self) -> bool;
}

impl ReadOnly for Vec<String> {
    fn readonly(&self) -> bool {
        self.iter().any(|entry| entry == "ro")
    }
}

impl ReadOnly for &str {
    fn readonly(&self) -> bool {
        self.split(',').any(|entry| entry == "ro")
    }
}

/// Return mountinfo matching source and/or destination.
pub(crate) async fn find_mount(
    source: Option<String>,
    target: Option<String>,
) -> Option<MountInfo> {
    let blocking_task = runtime::spawn_blocking(move || {
        let mut found: Option<MountInfo> = None;

        for mount in SafeMountIter::get().unwrap().flatten() {
            if let Some(value) = source.clone() {
                if mount.source.to_string_lossy() == value {
                    if let Some(value) = target.clone() {
                        if mount.dest.to_string_lossy() == value {
                            found = Some(mount);
                        }
                        continue;
                    }
                    found = Some(mount);
                }
                continue;
            }
            if let Some(value) = target.clone() {
                if mount.dest.to_string_lossy() == value {
                    found = Some(mount);
                }
            }
        }

        found.map(MountInfo::from)
    });

    blocking_task.await.unwrap_or_else(|error| {
        error!("Failed to wait for the thread {error}");
        None
    })
}

/// Return all mounts for a matching source.
/// Optionally ignore the given destination path.
pub(crate) async fn find_src_mounts(source: String, dest_ignore: Option<String>) -> Vec<MountInfo> {
    let blocking_task = runtime::spawn_blocking(move || {
        SafeMountIter::get()
            .unwrap()
            .flatten()
            .filter(|mount| {
                mount.source.to_string_lossy() == source
                    && match dest_ignore.clone() {
                        None => true,
                        Some(ignore) => ignore != mount.dest.to_string_lossy(),
                    }
            })
            .collect()
    });

    blocking_task.await.unwrap_or_else(|error| {
        error!("Failed to wait for the thread {error}");
        vec![]
    })
}

/// Check if options in "first" are also present in "second",
/// but exclude values "ro" and "rw" from the comparison.
pub(super) fn subset(first: &[String], second: &[String]) -> bool {
    let set: HashSet<&String> = second.iter().collect();
    for entry in first {
        if entry == "ro" {
            continue;
        }
        if entry == "rw" {
            continue;
        }
        if !set.contains(entry) {
            return false;
        }
    }
    true
}

/// Return supported filesystems.
pub(crate) fn probe_filesystems() -> Vec<FileSystem> {
    vec![Fs::Xfs.into(), Fs::Ext4.into(), Fs::Btrfs.into()]
}

// Utility function to transform a vector of options
// to the format required by sys_mount::Mount::new()
fn parse(options: Vec<String>) -> (bool, String) {
    let mut list: Vec<String> = Vec::new();
    let mut readonly: bool = false;

    for entry in options {
        if entry == "ro" {
            readonly = true;
            continue;
        }

        if entry == "rw" {
            continue;
        }

        list.push(entry);
    }

    (readonly, list.join(","))
}

// Utility function used for displaying a list of options.
fn show(options: Vec<String>) -> String {
    let list: Vec<String> = options
        .iter()
        .filter(|value| value.as_str() != "rw")
        .cloned()
        .collect();

    if list.is_empty() {
        return String::from("none");
    }

    list.join(",")
}

/// Mount a device to a directory (mountpoint)
pub(crate) async fn filesystem_mount(
    device: String,
    target: String,
    fstype: FileSystem,
    options: Vec<String>,
) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();

    let (readonly, value) = parse(options.clone());

    if readonly {
        flags.insert(MountFlags::RDONLY);
    }

    let _fstype = fstype.clone();
    let _target = target.clone();
    let _device = device.clone();
    let blocking_task = runtime::spawn_blocking(move || {
        let fs = FilesystemType::Manual(fstype.as_ref());
        // I'm not certain if it's fine to pass "" so keep existing behaviour
        let mntbuilder = if value.is_empty() {
            Mount::builder()
        } else {
            Mount::builder().data(&value)
        }
        .fstype(fs)
        .flags(flags);
        mntbuilder.mount(device, target)
    });

    debug!(
        "Filesystem ({}) on device {} mounted onto target {} (options: {})",
        _fstype,
        _device,
        _target,
        show(options)
    );

    let mount = blocking_task.await??;
    Ok(mount)
}

/// Unmount a device from a directory (mountpoint)
/// Should not be used for removing bind mounts.
pub(crate) async fn filesystem_unmount(target: String) -> Result<(), Error> {
    let flags = UnmountFlags::empty();
    // read more about the umount system call and it's flags at `man 2 umount`
    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || unmount(target, flags));

    debug!("Target {} unmounted", _target);
    let _ = blocking_task.await??;

    Ok(())
}

/// Bind mount a source path to a target path.
/// Supports both directories and files.
pub(crate) async fn bind_mount(source: String, target: String, file: bool) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();

    flags.insert(MountFlags::BIND);

    if file {
        flags.insert(MountFlags::RDONLY);
    }

    let mntbuilder = Mount::builder()
        .fstype(FilesystemType::Manual("none"))
        .flags(flags);

    let _source = source.clone();
    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || mntbuilder.mount(source, target));

    debug!("Source {} bind mounted onto target {}", _source, _target);

    let mount = blocking_task.await??;
    Ok(mount)
}

/// Bind remount a path to modify mount options.
/// Assumes that target has already been bind mounted.
pub(crate) async fn bind_remount(target: String, options: Vec<String>) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();

    let (readonly, value) = parse(options.clone());

    flags.insert(MountFlags::BIND);

    if readonly {
        flags.insert(MountFlags::RDONLY);
    }

    flags.insert(MountFlags::REMOUNT);

    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || {
        let mntbuilder = if value.is_empty() {
            Mount::builder()
        } else {
            Mount::builder().data(&value)
        }
        .fstype(FilesystemType::Manual("none"))
        .flags(flags);
        mntbuilder.mount("none", target)
    });

    debug!(
        "Target {} bind remounted (options: {})",
        _target,
        show(options)
    );

    let mount = blocking_task.await??;
    Ok(mount)
}

/// Unmounts a path that has previously been bind mounted.
/// Should not be used for unmounting devices.
pub(crate) async fn bind_unmount(target: String) -> Result<(), Error> {
    let flags = UnmountFlags::empty();

    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || unmount(target, flags));

    debug!("Target {} bind unmounted", _target);

    let _ = blocking_task.await??;
    Ok(())
}

/// Remount existing mount as read only or read write.
pub(crate) async fn remount(target: String, ro: bool) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::REMOUNT);

    if ro {
        flags.insert(MountFlags::RDONLY);
    }

    let mntbuilder = Mount::builder()
        .fstype(FilesystemType::Manual("none"))
        .flags(flags);

    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || mntbuilder.mount("", target.clone()));

    debug!("Target {} remounted with {}", _target, flags.bits());

    let mount = blocking_task.await??;
    Ok(mount)
}

/// Mount a block device
pub(crate) async fn blockdevice_mount(
    source: String,
    target: String,
    readonly: bool,
) -> Result<Mount, Error> {
    debug!("Mounting {} ...", source);

    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::BIND);

    let mntbuilder = Mount::builder()
        .fstype(FilesystemType::Manual("none"))
        .flags(flags);

    let _source = source.clone();
    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || mntbuilder.mount(source, target));

    info!("Block device {} mounted to {}", _source, _target);

    if readonly {
        flags.insert(MountFlags::REMOUNT);
        flags.insert(MountFlags::RDONLY);

        let mntbuilder = Mount::builder()
            .fstype(FilesystemType::Manual(""))
            .flags(flags);
        let __target = _target.clone();
        let blocking_task = runtime::spawn_blocking(move || mntbuilder.mount("", _target));
        info!(
            "Remounted block device {} (readonly) to {}",
            _source, __target
        );

        let mount = blocking_task.await??;
        return Ok(mount);
    }

    let mount = blocking_task.await??;
    Ok(mount)
}

/// Unmount a block device.
pub(crate) async fn blockdevice_unmount(target: String) -> Result<(), Error> {
    let flags = UnmountFlags::empty();

    debug!(
        "Unmounting block device {} (flags={}) ...",
        target,
        flags.bits()
    );

    let _target = target.clone();
    let blocking_task = runtime::spawn_blocking(move || unmount(target.clone(), flags));
    let _ = blocking_task.await??;

    info!("block device at {} has been unmounted", _target);
    Ok(())
}

/// Waits until a device's filesystem is shutdown.
/// This is useful to know if it's safe to detach a device from a node or not as it seems that
/// even after a umount completes the filesystem and more specifically the filesystem's journal
/// might not be completely shutdown.
/// Specifically, this waits for the filesystem (eg: ext4) shutdown and the filesystem's journal
/// shutdown: jbd2.
pub(crate) async fn wait_fs_shutdown(device: &str, fstype: Option<String>) -> Result<(), Error> {
    let device_trim = device.replace("/dev/", "");

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(2);

    if let Some(fstype) = fstype {
        let proc_fs_str = format!("/proc/fs/{fstype}/{device_trim}");
        let proc_fs = std::path::Path::new(&proc_fs_str);
        wait_file_removal(proc_fs, start, timeout).await?;
    }

    let jbd2_pattern = format!("/proc/fs/jbd2/{device_trim}-*");
    let proc_jbd2 = glob::glob(&jbd2_pattern)
        .expect("valid pattern")
        .next()
        .and_then(|v| v.ok());
    if let Some(proc_jbd2) = proc_jbd2 {
        wait_file_removal(&proc_jbd2, start, timeout).await?;
    }

    Ok(())
}

/// Waits until a file is removed, up to a timeout.
async fn wait_file_removal(
    proc: &std::path::Path,
    start: std::time::Instant,
    timeout: std::time::Duration,
) -> Result<(), Error> {
    let check_interval = std::time::Duration::from_millis(200);
    let proc_str = proc.to_string_lossy().to_string();
    let mut exists = proc.exists();
    while start.elapsed() < timeout && exists {
        tracing::error!(proc = proc_str, "proc entry still exists");
        tokio::time::sleep(check_interval).await;
        exists = proc.exists();
    }
    match exists {
        false => Ok(()),
        true => Err(Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Timed out waiting for '{proc_str}' to be removed"),
        )),
    }
}

/// If the filesystem uuid doesn't match with the provided uuid, unmount the device.
pub(crate) async fn unmount_on_fs_id_diff(
    device_path: String,
    fs_staging_path: String,
    volume_uuid: Uuid,
) -> Result<(), String> {
    if let Ok(probed_uuid) = FileSystem::property(device_path.clone(), "UUID".to_string()) {
        if probed_uuid == volume_uuid.to_string() {
            return Ok(());
        }
    }
    filesystem_unmount(fs_staging_path.clone()).await.map_err(|error| {
        format!(
            "Failed to unmount on fs id difference, device {device_path} from {fs_staging_path} for {volume_uuid}, {error}",
        )
    })
}
