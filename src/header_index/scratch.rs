//! Owned scratch indexes: liveness follows an OS lock, never a stored PID.
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

const PREFIX: &str = ".rbtc-header-index-";
const OWNER: &str = "owner";
const MAGIC: &[u8] = b"rbtc-derived-header-index-v1\n";
const MAX_SCAN: usize = 4_096;
const MAX_REMOVALS: usize = 64;

pub(super) struct Scratch {
    // Drop the file before TempDir removal (including on Windows).
    owner: Option<File>,
    directory: Option<tempfile::TempDir>,
}
impl Scratch {
    pub(super) fn path(&self) -> &Path {
        self.directory
            .as_ref()
            .expect("live scratch directory")
            .path()
    }
    pub(super) fn create(parent: &Path) -> io::Result<Self> {
        // Serialize creation and collection, including the marker-free creation
        // interval. The lock file is permanent and must not be unlinked.
        let _lock = parent_lock(parent)?;
        collect(parent)?;
        let directory = tempfile::Builder::new().prefix(PREFIX).tempdir_in(parent)?;
        let mut owner = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(directory.path().join(OWNER))?;
        owner.try_lock_exclusive()?;
        owner.write_all(MAGIC)?;
        owner.sync_all()?;
        #[cfg(unix)]
        {
            File::open(directory.path())?.sync_all()?;
            File::open(parent)?.sync_all()?;
        }
        Ok(Self {
            owner: Some(owner),
            directory: Some(directory),
        })
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Serialize normal removal with collection, so a disappearing directory
        // cannot turn another node's scratch creation into a spurious failure.
        let _lock = self
            .directory
            .as_ref()
            .and_then(|directory| directory.path().parent())
            .and_then(|parent| parent_lock(parent).ok());
        drop(self.owner.take());
        if let Some(directory) = self.directory.take() {
            let _ = directory.close();
        }
    }
}

fn parent_lock(parent: &Path) -> io::Result<File> {
    let path = parent.join(".rbtc-header-scratch.lock");
    match fs::symlink_metadata(&path) {
        Ok(_) if !regular(&path)? => {
            return Err(io::Error::other(
                "header scratch lock is not a private regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    lock.lock_exclusive()?;
    Ok(lock)
}

fn regular(path: &Path) -> io::Result<bool> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn collect(parent: &Path) -> io::Result<()> {
    let mut removed = 0;
    for entry in fs::read_dir(parent)?.take(MAX_SCAN) {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with(PREFIX) || !entry.file_type()?.is_dir()
        {
            continue;
        }
        let path = entry.path();
        let marker = path.join(OWNER);
        // Old unmarked directories and unfamiliar contents are not ours to
        // remove. No recursion and no symlink traversal is permitted.
        if !marker.try_exists()? || !regular(&marker)? {
            continue;
        }
        let mut owner = OpenOptions::new().read(true).write(true).open(&marker)?;
        match owner.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
        let mut magic = [0; MAGIC.len()];
        if owner.metadata()?.len() != MAGIC.len() as u64
            || owner.read_exact(&mut magic).is_err()
            || magic != MAGIC
        {
            continue;
        }
        let mut recognized = true;
        for child in fs::read_dir(&path)?.take(3) {
            let child = child?;
            if (child.file_name() != OWNER && child.file_name() != "index.redb")
                || !regular(&child.path())?
            {
                recognized = false;
                break;
            }
        }
        if !recognized {
            continue;
        }
        let index = path.join("index.redb");
        if index.try_exists()? {
            fs::remove_file(index)?;
        }
        // Other collectors/creators still hold off on the parent lock.
        drop(owner);
        fs::remove_file(marker)?;
        fs::remove_dir(path)?;
        removed += 1;
        if removed == MAX_REMOVALS {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_preserves_live_and_unrecognized_directories() {
        let root = tempfile::tempdir().unwrap();
        let live = Scratch::create(root.path()).unwrap();
        let live_path = live.path().to_path_buf();
        let old = root.path().join(format!("{PREFIX}unmarked"));
        fs::create_dir(&old).unwrap();
        fs::write(old.join("index.redb"), b"preserve").unwrap();
        let unknown = root.path().join(format!("{PREFIX}unknown"));
        fs::create_dir(&unknown).unwrap();
        fs::write(unknown.join(OWNER), MAGIC).unwrap();
        fs::write(unknown.join("extra"), b"preserve").unwrap();
        let next = Scratch::create(root.path()).unwrap();
        assert!(live_path.exists());
        assert!(old.join("index.redb").exists());
        assert!(unknown.join("extra").exists());
        drop(next);
        drop(live);
        assert!(!live_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn collection_refuses_links_and_preserves_their_targets() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let victim = root.path().join("canonical.redb");
        fs::write(&victim, b"canonical").unwrap();
        for name in ["linked-index", "linked-owner"] {
            let directory = root.path().join(format!("{PREFIX}{name}"));
            fs::create_dir(&directory).unwrap();
            if name == "linked-index" {
                fs::write(directory.join(OWNER), MAGIC).unwrap();
                symlink(&victim, directory.join("index.redb")).unwrap();
            } else {
                symlink(&victim, directory.join(OWNER)).unwrap();
            }
        }
        let live = Scratch::create(root.path()).unwrap();
        assert_eq!(fs::read(&victim).unwrap(), b"canonical");
        drop(live);
        fs::remove_file(root.path().join(".rbtc-header-scratch.lock")).unwrap();
        symlink(&victim, root.path().join(".rbtc-header-scratch.lock")).unwrap();
        assert!(Scratch::create(root.path()).is_err());
        assert_eq!(fs::read(victim).unwrap(), b"canonical");
    }

    #[test]
    #[ignore = "subprocess helper; invoked only by killed_owner_is_collected"]
    fn crash_child() {
        let Some(root) = std::env::var_os("RBTC_SCRATCH_CHILD_ROOT") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let index = crate::header_index::DiskHeaderIndex::create_scratch(
            &root,
            crate::deployments::DeploymentConfig::for_network(bitcoin::Network::Regtest),
        )
        .unwrap();
        fs::write(
            root.join("ready.tmp"),
            index
                .scratch
                .as_ref()
                .unwrap()
                .path()
                .as_os_str()
                .as_encoded_bytes(),
        )
        .unwrap();
        fs::rename(root.join("ready.tmp"), root.join("ready")).unwrap();
        loop {
            std::thread::park_timeout(std::time::Duration::from_secs(1));
        }
    }

    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn killed_owner_is_collected() {
        let root = tempfile::tempdir().unwrap();
        let mut child = ChildGuard(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "header_index::scratch::tests::crash_child",
                    "--ignored",
                ])
                .env("RBTC_SCRATCH_CHILD_ROOT", root.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let ready = root.path().join("ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !ready.exists() && std::time::Instant::now() < deadline {
            if child.0.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        if !ready.exists() {
            let _ = child.0.kill();
            let _ = child.0.wait();
            panic!("scratch subprocess did not become ready");
        }
        let path = std::path::PathBuf::from(fs::read_to_string(ready).unwrap());
        let concurrent = Scratch::create(root.path()).unwrap();
        assert!(path.join("index.redb").exists());
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let restarted = Scratch::create(root.path()).unwrap();
        assert!(!path.exists());
        assert!(concurrent.path().exists());
        assert!(restarted.path().exists());
    }
}
