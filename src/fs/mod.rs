//! Async access to the filesystem, in the shape of [`std::fs`].
//!
//! Each operation runs as blocking work through [`unblock()`], on the same pool, so it does not
//! block the thread that polls a task. A regular file cannot be waited on for readiness as a socket
//! can, so blocking work is how files are kept off that thread. The module needs no runtime and
//! works under any executor.
//!
//! The functions have the names of the ones in std, from [`read`] to [`write()`]. They take paths
//! as `AsRef<Path>` and fail with the errors of the std function they run. [`OpenOptions`],
//! [`DirBuilder`], [`DirEntry`] and [`File`] are built and used like their std counterparts.
//! Platform-specific options come from extension traits in the `unix` and `windows` modules, each
//! present only on its own platform. They match the extension traits of std. Code written for
//! `smol::fs` usually needs only a changed import path.
//!
//! # Dropping a future
//!
//! Each function converts its arguments to owned values and submits its work to the pool when its
//! future is first polled. From then on the work runs to the end, even if the future is no longer
//! polled or is dropped. Dropping a future gives up the wait for the result, not the work. A
//! [`remove_dir_all`] that is dropped keeps removing, and a [`write()`] still writes.
//!
//! If the work is a single call, it is either done or not done. If it is many calls, as in a
//! recursive removal, a crash or a full disk can leave it half done, as in std.
//!
//! # Files
//!
//! A [`File`] reads and writes through [`Unblock`](crate::Unblock), so it implements the
//! `AsyncRead`, `AsyncWrite` and `AsyncSeek` traits of `futures-io`. A read reads ahead of the
//! buffer it was given. A write completes once its bytes are passed to the pool. The [`File`] docs
//! explain what that means for the file, and for bytes written when it is dropped.
//!
//! # Example
//!
//! The example works in a directory created for it. It drives the futures with `block_on` from the
//! `futures` crate, but the `block_on` of any executor works, as the module needs no runtime:
//!
//! ```
//! use futures::{AsyncReadExt, AsyncWriteExt, executor::block_on};
//! use zruntime::fs::{self, File};
//!
//! # let pid = std::process::id();
//! # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-module-{pid}"));
//! # std::fs::create_dir_all(&dir).unwrap();
//! block_on(async {
//!     let path = dir.join("greeting.txt");
//!
//!     let mut file = File::create(&path).await?;
//!     file.write_all(b"hello, world").await?;
//!     file.flush().await?;
//!
//!     assert_eq!(fs::read_to_string(&path).await?, "hello, world");
//!     assert_eq!(fs::metadata(&path).await?.len(), 12);
//!
//!     fs::rename(&path, dir.join("farewell.txt")).await?;
//!     assert!(fs::metadata(&path).await.is_err());
//!
//!     let mut text = String::new();
//!     File::open(dir.join("farewell.txt")).await?.read_to_string(&mut text).await?;
//!     assert_eq!(text, "hello, world");
//!
//!     std::io::Result::Ok(())
//! })
//! .unwrap();
//! # std::fs::remove_dir_all(&dir).unwrap();
//! ```
//!
//! # Differences from `async-fs`
//!
//! This module is modelled on [`async-fs`], the crate behind `smol::fs`. It differs in these ways:
//!
//! * It uses this crate's pool and adapter, not those of `blocking`. An idle pool thread exits
//!   after ten seconds, and there are at most 500 threads. See [`unblock()`].
//! * [`File::metadata`] waits for the bytes written so far to reach the file, as [`File::sync_all`]
//!   and [`File::set_len`] do. The length it reports includes every earlier write. In `async-fs`,
//!   it reports the file as it is at that moment, which may be short of a write still in progress.
//! * [`File`] does not track a logical position or whether a write is waiting to be flushed. A
//!   flush always goes to the pool, so flushing a file that was never written to costs one piece of
//!   blocking work and does nothing else.
//! * [`ReadDir`] pulls up to 16 entries from the directory per piece of blocking work. `async-fs`
//!   does one per entry.
//! * The futures of [`DirBuilder::create`] and [`OpenOptions::open`] do nothing until polled, like
//!   those of the functions. In `async-fs`, the first starts its work at once.
//!
//! [`async-fs`]: https://crates.io/crates/async-fs

mod dir;
mod file;
mod options;
#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

use std::{
    io,
    path::{Path, PathBuf},
};

pub use dir::{DirBuilder, DirEntry, ReadDir};
pub use file::File;
pub use options::OpenOptions;
#[doc(no_inline)]
pub use std::fs::{FileType, Metadata, Permissions};

use crate::unblock;

/// Resolves `path` to its canonical form: absolute, with every `.` and `..` resolved and every
/// symbolic link followed.
///
/// Runs [`std::fs::canonicalize`] as blocking work.
///
/// # Errors
///
/// Fails if `path`, or any directory on the way to it, does not exist.
pub async fn canonicalize<P>(path: P) -> io::Result<PathBuf>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::canonicalize(path)).await
}

/// Copies the contents and the permissions of the file `src` to `dst`, and returns the number of
/// bytes copied.
///
/// Runs [`std::fs::copy`] as blocking work. An existing `dst` is overwritten. If `dst` is the same
/// file as `src`, it is likely to be truncated. To copy between two open [`File`]s, use an async
/// copy that reads and writes through them, such as `futures::io::copy`.
pub async fn copy<P, Q>(src: P, dst: Q) -> io::Result<u64>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::fs::copy(src, dst)).await
}

/// Creates a new, empty directory at `path`.
///
/// Runs [`std::fs::create_dir`] as blocking work.
///
/// # Errors
///
/// Fails if the parent of `path` does not exist, or if `path` already exists. To create missing
/// parents and accept an existing directory, use [`create_dir_all`].
pub async fn create_dir<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::create_dir(path)).await
}

/// Creates a directory at `path`, along with any parents that do not exist.
///
/// Runs [`std::fs::create_dir_all`] as blocking work. It is not an error if the directory already
/// exists, or if another thread or process creates it in the meantime.
pub async fn create_dir_all<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::create_dir_all(path)).await
}

/// Makes `dst` another name for the file `src`.
///
/// Runs [`std::fs::hard_link`] as blocking work. Both names refer to the same file. Most operating
/// systems allow this only for two names on the same filesystem.
pub async fn hard_link<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::fs::hard_link(src, dst)).await
}

/// Reads the metadata of the file or directory at `path`, following symbolic links.
///
/// Runs [`std::fs::metadata`] as blocking work. To read the metadata of a link itself, use
/// [`symlink_metadata`].
pub async fn metadata<P>(path: P) -> io::Result<Metadata>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::metadata(path)).await
}

/// Reads the whole file at `path` into a vector of bytes.
///
/// Runs [`std::fs::read`] as blocking work. It sizes the vector by the length of the file where it
/// can, and does all of its reading in one piece of work. That makes it faster than opening a
/// [`File`] and reading that, which takes one piece of work per chunk read. To read text, use
/// [`read_to_string`].
pub async fn read<P>(path: P) -> io::Result<Vec<u8>>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::read(path)).await
}

/// Opens the directory at `path` and returns a stream of its entries.
///
/// Runs [`std::fs::read_dir`] as blocking work. The entries come out of [`ReadDir`] in no
/// particular order.
///
/// # Errors
///
/// Fails if `path` is not a directory that can be read. Reading the [`ReadDir`] stream can fail
/// too.
///
/// # Example
///
/// ```
/// use futures::{StreamExt, executor::block_on};
/// use zruntime::fs;
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-read-dir-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     fs::write(dir.join("a.txt"), "first").await?;
///     fs::create_dir(dir.join("b")).await?;
///
///     let mut entries = fs::read_dir(&dir).await?;
///     let mut names = Vec::new();
///     while let Some(entry) = entries.next().await {
///         let entry = entry?;
///         names.push((entry.file_name(), entry.file_type().await?.is_dir()));
///     }
///     names.sort();
///
///     assert_eq!(names, [("a.txt".into(), false), ("b".into(), true)]);
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
pub async fn read_dir<P>(path: P) -> io::Result<ReadDir>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    let dir = unblock(move || std::fs::read_dir(path)).await?;
    Ok(ReadDir::new(dir))
}

/// Reads the target of the symbolic link at `path`.
///
/// Runs [`std::fs::read_link`] as blocking work. The path it returns is the one stored in the link.
/// It can be relative to the directory of the link, and can lead nowhere.
pub async fn read_link<P>(path: P) -> io::Result<PathBuf>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::read_link(path)).await
}

/// Reads the whole file at `path` into a string.
///
/// Runs [`std::fs::read_to_string`] as blocking work. To read bytes, use [`read`].
///
/// # Errors
///
/// Fails with [`InvalidData`](io::ErrorKind::InvalidData) if the file is not valid UTF-8.
pub async fn read_to_string<P>(path: P) -> io::Result<String>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::read_to_string(path)).await
}

/// Removes the directory at `path`, which must be empty.
///
/// Runs [`std::fs::remove_dir`] as blocking work. To remove a directory together with its contents,
/// use [`remove_dir_all`].
pub async fn remove_dir<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::remove_dir(path)).await
}

/// Removes the directory at `path` and everything in it.
///
/// Runs [`std::fs::remove_dir_all`] as blocking work. A large directory takes a while to remove,
/// and the polling thread stays free meanwhile. If the removal fails part of the way, what was not
/// yet removed stays where it is.
pub async fn remove_dir_all<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::remove_dir_all(path)).await
}

/// Removes the file at `path`.
///
/// Runs [`std::fs::remove_file`] as blocking work. It removes a name. For a symbolic link, that is
/// the link and not its target.
pub async fn remove_file<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::remove_file(path)).await
}

/// Renames the file or directory at `src` to `dst`, replacing what is at `dst` if the platform
/// allows it.
///
/// Runs [`std::fs::rename`] as blocking work.
///
/// # Errors
///
/// Fails, for example, if `src` and `dst` are on different filesystems.
pub async fn rename<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref().to_owned();
    let dst = dst.as_ref().to_owned();
    unblock(move || std::fs::rename(src, dst)).await
}

/// Changes the permissions of the file or directory at `path` to `perm`.
///
/// Runs [`std::fs::set_permissions`] as blocking work. To change only some of the permissions,
/// start from the ones that [`metadata`] reads.
pub async fn set_permissions<P>(path: P, perm: Permissions) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::set_permissions(path, perm)).await
}

/// Reads the metadata of the file, directory or symbolic link at `path`, without following a link.
///
/// Runs [`std::fs::symlink_metadata`] as blocking work. To read the metadata of what a link points
/// at, use [`metadata`].
pub async fn symlink_metadata<P>(path: P) -> io::Result<Metadata>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_owned();
    unblock(move || std::fs::symlink_metadata(path)).await
}

/// Writes `contents` to the file at `path`, creating the file if it does not exist and replacing
/// its contents if it does.
///
/// Runs [`std::fs::write`] as blocking work. The bytes are copied when the future is first polled,
/// because the work runs on another thread and cannot borrow them from the caller.
pub async fn write<P, C>(path: P, contents: C) -> io::Result<()>
where
    P: AsRef<Path>,
    C: AsRef<[u8]>,
{
    let path = path.as_ref().to_owned();
    let contents = contents.as_ref().to_owned();
    unblock(move || std::fs::write(path, contents)).await
}

/// What the `unix` and `windows` modules seal their extension traits with, out of reach of every
/// crate but this one.
///
/// The trait is public in name only, so that the extension traits can name it as their
/// supertrait, and sits in a module nobody outside can reach, so that nobody outside can implement
/// it: the types of this module that it is implemented for are the only ones the extension traits
/// are meant for.
pub(crate) mod sealed {
    /// Marks a type of this module that an extension trait is implemented for.
    pub trait Sealed {}
}
