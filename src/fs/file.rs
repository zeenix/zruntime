//! [`File`], an open file with async reads and writes.

use std::{
    fmt, future,
    io::{self, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, OwnedHandle, RawHandle};

use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};

use super::{Metadata, Permissions};
use crate::{Unblock, lock::Mutex, unblock};

/// An open file whose reads, writes and seeks run as blocking work.
///
/// Open a file with [`open`](File::open), [`create`](File::create) or
/// [`OpenOptions`](super::OpenOptions), or convert a [`std::fs::File`] with [`From`]. `File`
/// implements `futures-io`'s [`AsyncRead`], [`AsyncWrite`] and [`AsyncSeek`] through an
/// [`Unblock`](crate::Unblock) over the file. The other methods of std's file are `async` methods
/// here.
///
/// Like [`unblock()`], it needs no runtime and works under any executor. It is `Send`, `Sync` and
/// `Unpin`.
///
/// The file is closed when it is dropped. An error from closing it is lost, so call
/// [`sync_all`](File::sync_all) before dropping the file to learn of one.
///
/// # Reading
///
/// A read has the pool read up to 64 KiB from the file at once, whatever the size of the buffer it
/// was given. The bytes the buffer does not take are kept for the reads that follow. So the
/// position the OS keeps for the file is ahead of where the reads got to.
///
/// The position that a seek reports and seeks from, including `SeekFrom::Current`, is where the
/// reads got to. A write goes there too. If the file was read from since its position was last put
/// right, it moves the OS position back there before it writes.
///
/// # Writing
///
/// A write takes up to 64 KiB of the given bytes. It completes as soon as it has passed them to the
/// pool, which writes all of them to the file. The next read, write, seek or flush waits for that
/// work to finish first. So the bytes are written in the order they were given, and a file reads
/// back what was written. If the work failed, the next write or flush returns the error. A
/// [flush](futures_io::AsyncWrite::poll_flush) waits for every earlier write to finish.
///
/// Once a write completes, the pool holds its bytes and nothing waits on the file. If the file is
/// dropped right after a write, it stays open until the pool is done with it, and every byte
/// reaches the file, but an error goes unreported. To learn of errors, flush the file or call
/// [`sync_all`](File::sync_all) before dropping it. The same applies to a file dropped by a task
/// that is cancelled.
///
/// The methods that look at the file or change it as a whole, [`sync_all`](File::sync_all),
/// [`sync_data`](File::sync_data), [`set_len`](File::set_len) and [`metadata`](File::metadata),
/// take `&self`. Each first waits for the earlier writes to finish, and returns the error of a
/// failed one, as a flush does. When several tasks call them at once, the tasks take turns waiting
/// for the writes.
///
/// # Raw handles
///
/// On unix, the file implements `AsFd` and `AsRawFd`. On Windows, it implements `AsHandle` and
/// `AsRawHandle`. They give the raw descriptor or handle, for calls that std or the OS has and this
/// type does not. The file does not account for anything done through them. The OS position can be
/// ahead of where the reads got to, as described above, and the file does not wait for writes in
/// progress.
///
/// # Example
///
/// This example writes to a file, then reads it through the same handle from a position it seeks
/// to. It drives the futures with `block_on` from the `futures` crate, but the `block_on` of any
/// executor works, as the file needs no runtime:
///
/// ```
/// use std::io::SeekFrom;
///
/// use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, executor::block_on};
/// use zruntime::fs::OpenOptions;
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-file-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     let mut file = OpenOptions::new()
///         .read(true)
///         .write(true)
///         .create(true)
///         .open(dir.join("notes.txt"))
///         .await?;
///
///     file.write_all(b"hello, world").await?;
///     // The length includes the bytes written, even though the file was not flushed.
///     assert_eq!(file.metadata().await?.len(), 12);
///
///     file.seek(SeekFrom::Start(7)).await?;
///     let mut word = String::new();
///     file.read_to_string(&mut word).await?;
///     assert_eq!(word, "world");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
///
/// A write that follows a read lands where the reads got to, not where the read-ahead got to:
///
/// ```
/// use futures::{AsyncReadExt, AsyncWriteExt, executor::block_on};
/// use zruntime::fs::{self, OpenOptions};
///
/// # let pid = std::process::id();
/// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-file-write-{pid}"));
/// # std::fs::create_dir_all(&dir).unwrap();
/// block_on(async {
///     let path = dir.join("greeting.txt");
///     fs::write(&path, "hello, world").await?;
///
///     let mut file = OpenOptions::new().read(true).write(true).open(&path).await?;
///     file.read_exact(&mut [0; 5]).await?;
///     file.write_all(b"!").await?;
///     file.flush().await?;
///
///     assert_eq!(fs::read_to_string(&path).await?, "hello! world");
///
///     std::io::Result::Ok(())
/// })
/// .unwrap();
/// # std::fs::remove_dir_all(&dir).unwrap();
/// ```
pub struct File {
    /// The file, for the operations that are not reads, writes or seeks, and for the raw handle.
    /// The adapter holds the same file, in an [`ArcFile`], to run its operations on.
    file: Arc<std::fs::File>,
    /// The adapter that runs the reads, writes and seeks as blocking work.
    ///
    /// The methods that take `&self` need the adapter to flush it, which takes a mutable borrow,
    /// so it sits in a mutex. The ones that take `&mut self` reach it without locking.
    unblock: Mutex<Unblock<ArcFile>>,
    /// Whether a read has been started since the position of the file was last put right, which
    /// means the adapter may have read ahead of where the reads got to.
    read_ahead: bool,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// Runs [`std::fs::File::open`] as blocking work. To open a file in other ways, use
    /// [`OpenOptions`](super::OpenOptions).
    ///
    /// # Errors
    ///
    /// Fails if there is no file at `path`, or if the process may not read it.
    pub async fn open<P>(path: P) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_owned();
        let file = unblock(move || std::fs::File::open(path)).await?;
        Ok(File::from(file))
    }

    /// Opens the file at `path` for writing, creating it if it does not exist and emptying it if it
    /// does.
    ///
    /// Runs [`std::fs::File::create`] as blocking work. The file cannot be read through the
    /// returned handle. To open one that can, use [`OpenOptions`](super::OpenOptions).
    ///
    /// # Errors
    ///
    /// Fails if the directory of the file does not exist, or if the process may not write to the
    /// file.
    pub async fn create<P>(path: P) -> io::Result<File>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_owned();
        let file = unblock(move || std::fs::File::create(path)).await?;
        Ok(File::from(file))
    }

    /// Waits for the writes so far to reach the file, then asks the OS to write the data and
    /// metadata of the file to disk.
    ///
    /// Runs [`std::fs::File::sync_all`] as blocking work after a flush. It tells whether the file
    /// reached the disk. It also reports an error that a write, or closing the file, would
    /// otherwise lose.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::{AsyncWriteExt, executor::block_on};
    /// use zruntime::fs::File;
    ///
    /// # let pid = std::process::id();
    /// # let dir = std::env::temp_dir().join(format!("zruntime-fs-doc-sync-all-{pid}"));
    /// # std::fs::create_dir_all(&dir).unwrap();
    /// block_on(async {
    ///     let mut file = File::create(dir.join("journal.txt")).await?;
    ///     file.write_all(b"entry").await?;
    ///     file.sync_all().await?;
    ///
    ///     std::io::Result::Ok(())
    /// })
    /// .unwrap();
    /// # std::fs::remove_dir_all(&dir).unwrap();
    /// ```
    pub async fn sync_all(&self) -> io::Result<()> {
        self.flush_writes().await?;
        let file = self.file.clone();
        unblock(move || file.sync_all()).await
    }

    /// Waits for the writes so far to reach the file, then asks the OS to write the data of the
    /// file to disk.
    ///
    /// Runs [`std::fs::File::sync_data`] as blocking work after a flush. The metadata is written
    /// only if it is needed to read the data back. This does less than
    /// [`sync_all`](File::sync_all) on a platform that can tell the two apart, and the same on one
    /// that cannot.
    pub async fn sync_data(&self) -> io::Result<()> {
        self.flush_writes().await?;
        let file = self.file.clone();
        unblock(move || file.sync_data()).await
    }

    /// Waits for the writes so far to reach the file, then truncates or extends the file to `size`
    /// bytes.
    ///
    /// Runs [`std::fs::File::set_len`] as blocking work after a flush. An extended file is filled
    /// with zeros. The position of the file does not change, even if it is now past the end. The
    /// bytes that earlier reads read ahead are dropped, so later reads see the file as it is now,
    /// from that position.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        let mut adapter = self.unblock.lock().await;
        future::poll_fn(|cx| Pin::new(&mut *adapter).poll_flush(cx)).await?;
        if self.read_ahead {
            // Drops the bytes read ahead, which may be of the part that is cut off, or that is
            // extended again with zeros, and puts the position of the OS back to where the reads
            // got to, as `poll_reposition` does before a write. Its outcome is of no interest
            // there, and none here: a file that cannot be sought cannot be cut either, which the
            // `set_len` below reports.
            let current = SeekFrom::Current(0);
            let _ = future::poll_fn(|cx| Pin::new(&mut *adapter).poll_seek(cx, current)).await;
        }

        let file = self.file.clone();
        unblock(move || file.set_len(size)).await
    }

    /// Waits for the writes so far to reach the file, then reads the metadata of the file.
    ///
    /// Runs [`std::fs::File::metadata`] as blocking work after a flush, so the length it reports
    /// includes every byte written before the call.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        self.flush_writes().await?;
        let file = self.file.clone();
        unblock(move || file.metadata()).await
    }

    /// Changes the permissions of the file to `perm`.
    ///
    /// Runs [`std::fs::File::set_permissions`] as blocking work. Unlike the methods above, it does
    /// not wait for earlier writes, because the permissions do not depend on the bytes written.
    pub async fn set_permissions(&self, perm: Permissions) -> io::Result<()> {
        let file = self.file.clone();
        unblock(move || file.set_permissions(perm)).await
    }

    /// Waits for every write so far to be over, and for the adapter to flush the file.
    ///
    /// The flush leaves what the adapter read ahead where it is, which running the operation on
    /// the file through the adapter would not.
    async fn flush_writes(&self) -> io::Result<()> {
        let mut unblock = self.unblock.lock().await;
        future::poll_fn(|cx| Pin::new(&mut *unblock).poll_flush(cx)).await
    }

    /// Puts the position of the OS back to where the reads got to, if a read may have moved it
    /// past that.
    ///
    /// A seek by `SeekFrom::Current(0)` through the adapter does that: it accounts for the bytes
    /// read ahead. Its outcome is of no interest. A file that cannot be sought, a pipe or a
    /// socket, has no position to put right, and its reads and writes are separate streams, so the
    /// bytes read ahead are kept for the reads that follow, as the adapter keeps them when a seek
    /// fails. One that can be sought does not fail to report where it is.
    fn poll_reposition(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.read_ahead {
            let _ = ready!(Pin::new(self.unblock.get_mut()).poll_seek(cx, SeekFrom::Current(0)));
            self.read_ahead = false;
        }
        Poll::Ready(())
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.file, f)
    }
}

impl From<std::fs::File> for File {
    /// Converts a [`std::fs::File`] into an async `File`, keeping its current position.
    fn from(file: std::fs::File) -> Self {
        let file = Arc::new(file);
        Self {
            unblock: Mutex::new(Unblock::with_capacity(CAPACITY, ArcFile(file.clone()))),
            file,
            read_ahead: false,
        }
    }
}

#[cfg(unix)]
impl From<OwnedFd> for File {
    fn from(fd: OwnedFd) -> Self {
        Self::from(std::fs::File::from(fd))
    }
}

#[cfg(windows)]
impl From<OwnedHandle> for File {
    fn from(handle: OwnedHandle) -> Self {
        Self::from(std::fs::File::from(handle))
    }
}

#[cfg(unix)]
impl AsFd for File {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsHandle for File {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.file.as_handle()
    }
}

#[cfg(windows)]
impl AsRawHandle for File {
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Set before the read is polled, as one that is pending, or that is given up on, has the
        // pool read ahead all the same.
        self.read_ahead = true;
        Pin::new(self.unblock.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.poll_reposition(cx));
        Pin::new(self.unblock.get_mut()).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.unblock.get_mut()).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The file is closed when it is dropped, which is after the writes the flush waits for.
        Pin::new(self.unblock.get_mut()).poll_close(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        // The adapter takes the bytes read ahead into account, including for a seek from the
        // current position, so there is nothing to put right first.
        let pos = ready!(Pin::new(self.unblock.get_mut()).poll_seek(cx, pos))?;
        // A seek that worked leaves nothing read ahead. One that failed leaves it, and the flag.
        self.read_ahead = false;
        Poll::Ready(Ok(pos))
    }
}

/// The file of std behind an [`Arc`], for the adapter to read, write and seek: through the
/// implementations of std's traits for a reference to a file, which `Arc<File>` does not have.
///
/// The adapter owns the clone of the `Arc` that this holds, and so keeps the file open until its
/// last operation is over, even if the [`File`] was dropped meanwhile.
struct ArcFile(Arc<std::fs::File>);

impl Read for ArcFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self.0).read(buf)
    }
}

impl Write for ArcFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self.0).flush()
    }
}

impl Seek for ArcFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        (&*self.0).seek(pos)
    }
}

/// The most bytes that one operation of a file reads ahead or writes.
///
/// Each operation is a hand-over to the pool, which takes tens of microseconds, so a larger
/// capacity is faster for a large file, and a smaller one is cheaper for a file that is read for a
/// few bytes only: the buffer of a read is zeroed in full when the first read of the file begins.
/// On a 64 MiB file in the page cache, reading went at 0.3 GB/s with the 8 KiB that
/// [`Unblock::new`] has, at 1.6 GB/s with 64 KiB, and at 3.5 GB/s with 256 KiB, while opening a
/// file and reading ten bytes of it took 54, 65 and 85 microseconds. This is the middle one: most
/// of the speed of the larger, and little of the cost of the smaller.
const CAPACITY: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();
