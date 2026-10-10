# zruntime

[![CI Pipeline Status](https://github.com/z-galaxy/zruntime/actions/workflows/rust.yml/badge.svg)](https://github.com/z-galaxy/zruntime/actions/workflows/rust.yml)
[![](https://docs.rs/zruntime/badge.svg)](https://docs.rs/zruntime/)
[![](https://img.shields.io/crates/v/zruntime)](https://crates.io/crates/zruntime)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/z-galaxy/zruntime?utm_source=badge)

A simple, single-threaded async runtime for Rust.

A [`Runtime`] has a scheduler, which runs tasks, and a reactor, which watches I/O sources and
keeps timers. [`Runtime::block_on`] runs a future to completion on the calling thread. While it
waits for the future, the same thread also runs the runtime's tasks, timers and I/O.

The reactor waits on epoll on Linux and Android, kqueue on the BSDs, `select` on Apple platforms
and Windows, and `poll(2)` on other unix systems.

A runtime comes in one of two flavours:

* [`LocalRuntime`] is the default. It stays on the thread that created it and runs any `'static`
  future. It keeps its state in `Rc` and `RefCell`, which makes it the cheaper of the two.
* [`SharedRuntime`] can be used from, and run on, any thread. It only runs `Send` futures, and
  keeps its state in `Arc` and `Mutex`.

Besides the runtime, the crate has async sockets, child processes, filesystem access and a thread
pool for blocking work. It also has an event, locks and channels that work under any executor.
Most of these are behind [cargo features](#cargo-features).

## Examples

A local runtime, used by the thread that created it:

```rust
use std::{cell::RefCell, rc::Rc, time::Duration};

use zruntime::LocalRuntime;

let runtime = LocalRuntime::new().expect("a runtime for this thread");
let doubled = runtime.block_on(async {
    let count = Rc::new(RefCell::new(0));
    let sleeper = runtime.clone();
    let task = runtime.spawn("double", {
        let count = count.clone();
        async move {
            sleeper.sleep(Duration::from_millis(1)).await;
            *count.borrow_mut() += 21;
            *count.borrow() * 2
        }
    });

    task.await.expect("the task did not panic")
});

assert_eq!(doubled, 42);
```

A shared runtime, spawned on from another thread while the main thread runs it:

```rust
use std::{sync::mpsc, thread};

use zruntime::SharedRuntime;

let runtime = SharedRuntime::new().expect("a runtime for this thread");
let (sender, receiver) = mpsc::channel();
let spawner = thread::spawn({
    let runtime = runtime.clone();
    move || {
        let task = runtime.spawn("double", async { 21 * 2 });
        sender.send(task).expect("the main thread is still waiting for it");
    }
});

let task = receiver.recv().expect("the other thread sent the task");
let doubled = runtime.block_on(task).expect("the task did not panic");
spawner.join().expect("the other thread did not panic");

assert_eq!(doubled, 42);
```

Create a runtime with `LocalRuntime::new()` or `SharedRuntime::new()`. The compiler cannot infer
the flavour of a plain `Runtime::new()`.

## Tasks

[`Runtime::spawn`] starts a task on that runtime. It takes a name, which the log message uses if
the task panics.

Code that has no runtime handle can use the free [`spawn_local`] and [`spawn`] functions instead.
They spawn on the runtime that is running the calling code, and name the task after the place it
was spawned from.

```rust
use zruntime::{LocalRuntime, Task};

// Has no runtime handle: it spawns on the runtime that runs its caller.
fn double(number: u32) -> Task<u32> {
    zruntime::spawn_local(async move { number * 2 })
}

let runtime = LocalRuntime::new().expect("a runtime for this thread");
let doubled = runtime.block_on(async { double(21).await.expect("the task did not panic") });

assert_eq!(doubled, 42);
```

* [`spawn_local`] takes any `'static` future. Call it from code running on a [`LocalRuntime`]:
  from the future passed to its `block_on`, or from one of its tasks.
* [`spawn`] takes a `Send` future, and spawns it on the [`SharedRuntime`] that runs the calling
  code. If there is none, it panics, unless the `helper` feature is on (see
  [Per-thread runtimes](#per-thread-runtimes)).

Both return a [`Task`], the handle of the task:

* Awaiting it returns the task's output, or an error if the task panicked or its runtime was
  dropped.
* Dropping it cancels the task.
* [`Task::cancel`] cancels the task and waits until it has stopped. If the task had already
  finished, it returns the output.
* [`Task::detach`] lets the task run on without a handle.
* [`Task::is_finished`] tells whether the task has ended, without taking its output.

## Timers

* [`Runtime::sleep`] waits for a duration, and [`Runtime::sleep_until`] waits until a deadline. A
  loop that sleeps until deadlines keeps to its schedule without drifting.
* [`Sleep::reset`] moves the deadline of a sleep, for example to restart an idle timeout.
* [`Runtime::timeout`] puts a time limit on any future.
* [`Runtime::interval`] ticks once every period. It is also a `Stream`.

```rust
use std::{future, time::Duration};

use zruntime::LocalRuntime;

let runtime = LocalRuntime::new().expect("a runtime for this thread");
runtime.block_on(async {
    let mut interval = runtime.interval(Duration::from_millis(1));
    for _ in 0..3 {
        interval.tick().await;
    }

    let never = runtime.timeout(Duration::from_millis(2), future::pending::<()>());
    assert!(never.await.is_err());
});
```

## Sockets

The [`net`] module has TCP and UDP sockets: `TcpListener`, `TcpStream` and `UdpSocket`. On unix,
its `unix` module has `UnixListener`, `UnixStream` and `UnixDatagram`. They are like the sockets of
`smol::net`, and work on both flavours of runtime. Streams implement `AsyncRead` and `AsyncWrite`
from `futures-io`. Connecting never blocks the thread.

The sockets take socket addresses, not host names. To look up a name without blocking the thread,
run the lookup with [`unblock`].

## Other I/O sources

[`AsyncIo`] makes any source the reactor can watch async, like `smol::Async`. On unix, that is
anything with a file descriptor: a pipe, a terminal, an eventfd, an inotify instance, or a socket
type that [`net`] does not cover. On Windows, it can only be a socket.

* [`AsyncIo::read_with`] and [`AsyncIo::write_with`] run an operation on the source until it no
  longer fails with `WouldBlock`.
* [`AsyncIo::readable`] and [`AsyncIo::writable`] only wait for readiness. They are useful when
  another library does the I/O on the descriptor.
* `AsyncIo<T>` implements `AsyncRead` from `futures-io` if `&T` implements `Read`, and
  `AsyncWrite` if `&T` implements `Write`.

Any number of tasks can wait through these four methods at once. The `AsyncRead` and `AsyncWrite`
impls keep only one waiting task per direction.

`AsyncIo` is built on [`Runtime::register`] and [`Registration`], which are public too.

`AsyncIo` is not for regular files: reading or writing one can block the thread, whatever its
readiness says. Use [`Unblock`] or the [`fs`] module for files.

## Blocking work and files

[`unblock`] runs blocking work on a pool of threads, and returns a future of its result.
[`Unblock`] wraps a blocking I/O handle, such as a file or the standard input. It implements the
async I/O traits by running each operation on that pool.

The [`fs`] module is async filesystem access, in the shape of `std::fs` and like `smol::fs`. It
runs each operation on the same pool.

None of these needs a runtime: they work under any executor.

## Child processes

The [`process`] module runs child processes, like `smol::process`. Its `Command` works like
`std::process::Command`, but spawns the child on a runtime that you pass to it. The `Child` it
returns has async `status` and `output` methods. The pipes to the child, `ChildStdin`,
`ChildStdout` and `ChildStderr`, implement `AsyncWrite` and `AsyncRead`.

On unix, the reactor watches the pipes. On Windows, their I/O runs on the [`unblock`] pool.

Waiting for a child to exit never blocks the thread. On Linux, Apple platforms and the BSDs, the
reactor watches for the exit. Elsewhere, the wait runs on a separate pool of threads. Dropping a
`Child` leaves the process running, unless `kill_on_drop(true)` was set on its `Command`. The
[`process`] module documentation has the details.

## Events, locks and channels

These work under any executor, not only zruntime's. Tasks on different runtimes and threads can
share them.

* [`Event`] is a notification that tasks can wait for. A task gets an [`EventListener`] from the
  event and awaits it. Code that changes what the task waits for, for example by releasing a lock,
  notifies the event. That wakes as many listeners as it asks for, oldest first. The locks and
  channels below are built on it.
* The [`lock`] module has an async `Mutex`, `RwLock` and `Semaphore`, whose guards can be held
  across an `.await`. It also has a `Barrier`, and a `OnceCell` whose initialiser can be async.
* [`mpmc`] is a multi-producer, multi-consumer channel, bounded or unbounded. Each message goes to
  one receiver.
* [`broadcast`] is a multi-producer, multi-consumer broadcast channel. Each message goes to every
  receiver.

## Running on several threads

One thread at a time runs a runtime, so all its tasks share one CPU core. A task that keeps the
CPU busy holds up every other task on the same runtime.

Two threads cannot run one runtime together. If a thread calls `block_on` on a [`SharedRuntime`]
from `SharedRuntime::new` while another thread is running `block_on` on it, the call panics. On a
runtime from [`SharedRuntime::current`], the call waits for its turn instead.

To use more cores, use several runtimes, one per thread, each run by its own `block_on`. The free
[`block_on`] already works like this: each thread that calls it runs its own runtime.

To spread work over the threads, give each thread a clone of the same [`mpmc`] receiver. Each
piece of work goes to whichever thread receives it first, as [the module's example] shows. To run
a task on a specific thread, spawn it on that thread's runtime. A [`SharedRuntime`] can be spawned
on from any thread, as the second example above shows.

## Per-thread runtimes

The examples above run their runtime with one `block_on` call for the whole program. Some code
works differently. A library such as [zbus] may call `block_on` once per operation, or have its
futures polled by another executor. Its tasks and timers must keep running between those calls.

The `helper` feature supports this:

* The free [`block_on`] runs a future on the calling thread's own runtime.
* [`SharedRuntime::current`] returns the runtime for the calling code. Called from a task of such
  a runtime, or from the future passed to `block_on` on one, it returns that runtime. Called from
  the future passed to the free `block_on`, it returns the calling thread's own runtime. Called
  anywhere else, it returns one runtime that the whole process shares.
* If such a runtime has work to do and no thread is running `block_on` on it, a helper thread runs
  it. The helper starts when needed, and stops once nothing is left to run, watch or time.
* When a thread calls `block_on` while the helper is running the runtime, the helper hands the
  runtime over to that thread. So each operation still runs on the thread that called `block_on`.
* [`spawn`] works from any thread. If no shared runtime runs the calling code, the task goes on
  the runtime that [`SharedRuntime::current`] returns.

## Combinators

zruntime has no combinators of its own. Use those of the [`futures`] crate instead. They work with
zruntime's tasks, timers, sockets and pipes, which implement the standard `Future`, `Stream`,
`AsyncRead` and `AsyncWrite` traits.

* [`futures::future`] joins and races futures.
* [`futures::stream`] adapts streams, such as an interval or the connections of a listener.
* [`futures::io`] adds methods such as `read_to_end` and `write_all`, and a `BufReader` to read
  lines with.

The default features of `futures` bring an executor, which you do not need with zruntime. They
also bring the `join!` and `select!` macros, which build a proc-macro and the `syn` crate. To leave
both out:

```toml
[dependencies]
futures = { version = "0.3", default-features = false, features = ["std"] }
```

`std` brings the I/O extension traits. Add `async-await` to get the macros back.

```rust
use std::time::Duration;

use futures::future;
use zruntime::LocalRuntime;

let runtime = LocalRuntime::new().expect("a runtime for this thread");
runtime.block_on(async {
    // Two tasks after the same answer, one of them much faster than the other.
    let soon = runtime.clone();
    let near = runtime.spawn("near", async move {
        soon.sleep(Duration::from_millis(1)).await;
        "near"
    });
    let late = runtime.clone();
    let far = runtime.spawn("far", async move {
        late.sleep(Duration::from_secs(60)).await;
        "far"
    });

    // The first to answer wins. The other one is returned with the answer, and dropping it
    // cancels it.
    let (answer, _) = future::select(near, far).await.factor_first();
    assert_eq!(answer.expect("the task did not panic"), "near");

    // Two futures awaited together, keeping the outputs of both.
    let (a, b) = future::join(async { 21 }, async { 2 }).await;
    assert_eq!(a * b, 42);
});
```

To race a future against a timer, use [`Runtime::timeout`]. The [`Unblock`] documentation shows
how to read the standard input line by line through a `BufReader`.

## Cargo features

* `runtime` (default): [`Runtime`], [`LocalRuntime`], [`SharedRuntime`], tasks, timers,
  [`AsyncIo`] and [`Registration`].
* `event` (default): [`Event`] and [`EventListener`].
* `tracing` (default): log through [`tracing`]. Without it, the runtime logs nothing.
* `helper`: [per-thread runtimes](#per-thread-runtimes). Implies `runtime`.
* `lock`: the [`lock`] module. Implies `event`.
* `mpmc`: the [`mpmc`] module. Implies `event`.
* `broadcast`: the [`broadcast`] module. Implies `event`.
* `unblock`: [`unblock`] and [`Unblock`].
* `fs`: the [`fs`] module. Implies `unblock` and `lock`.
* `tcp`: `TcpListener` and `TcpStream` in the [`net`] module. Implies `runtime`.
* `udp`: `UdpSocket` in the [`net`] module. Implies `runtime`.
* `unix`: the `net::unix` module, on unix only. Implies `runtime`.
* `process`: the [`process`] module. Implies `runtime` and `unblock`.

`runtime` and `event` do not depend on each other. A crate that only needs `Event`, or the locks
and channels built on it, can turn off the default features and enable only what it uses, such as
`features = ["lock"]`. That builds none of the runtime, nor the OS crates it polls with.

## Why?

The project grew out of the need for a single-threaded runtime in [zbus] that it would use by
default. It was split into a separate project so non-zbus users can use it too.

## License

[MIT]

## Sponsors

<a href="https://codspeed.io/?utm_source=oss-sponsorship&utm_medium=z-galaxy">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://codspeed.io/codspeed-logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://codspeed.io/codspeed-logo-light.svg">
    <img alt="CodSpeed logo" src="https://codspeed.io/codspeed-logo-light.svg" width="400">
  </picture>
</a>

[`Runtime`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html
[`Runtime::block_on`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.block_on
[`LocalRuntime`]: https://docs.rs/zruntime/latest/zruntime/type.LocalRuntime.html
[`SharedRuntime`]: https://docs.rs/zruntime/latest/zruntime/type.SharedRuntime.html
[`SharedRuntime::current`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.current
[`block_on`]: https://docs.rs/zruntime/latest/zruntime/fn.block_on.html
[`Runtime::spawn`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.spawn
[`spawn`]: https://docs.rs/zruntime/latest/zruntime/fn.spawn.html
[`spawn_local`]: https://docs.rs/zruntime/latest/zruntime/fn.spawn_local.html
[`Task`]: https://docs.rs/zruntime/latest/zruntime/struct.Task.html
[`Task::cancel`]: https://docs.rs/zruntime/latest/zruntime/struct.Task.html#method.cancel
[`Task::detach`]: https://docs.rs/zruntime/latest/zruntime/struct.Task.html#method.detach
[`Task::is_finished`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Task.html#method.is_finished
[`Runtime::sleep`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.sleep
[`Runtime::sleep_until`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.sleep_until
[`Sleep::reset`]: https://docs.rs/zruntime/latest/zruntime/struct.Sleep.html#method.reset
[`Runtime::timeout`]: https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.timeout
[`Runtime::interval`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.interval
[`Runtime::register`]:
    https://docs.rs/zruntime/latest/zruntime/struct.Runtime.html#method.register
[`Registration`]: https://docs.rs/zruntime/latest/zruntime/struct.Registration.html
[`AsyncIo`]: https://docs.rs/zruntime/latest/zruntime/struct.AsyncIo.html
[`AsyncIo::readable`]:
    https://docs.rs/zruntime/latest/zruntime/struct.AsyncIo.html#method.readable
[`AsyncIo::writable`]:
    https://docs.rs/zruntime/latest/zruntime/struct.AsyncIo.html#method.writable
[`AsyncIo::read_with`]:
    https://docs.rs/zruntime/latest/zruntime/struct.AsyncIo.html#method.read_with
[`AsyncIo::write_with`]:
    https://docs.rs/zruntime/latest/zruntime/struct.AsyncIo.html#method.write_with
[`Event`]: https://docs.rs/zruntime/latest/zruntime/struct.Event.html
[`EventListener`]: https://docs.rs/zruntime/latest/zruntime/struct.EventListener.html
[`broadcast`]: https://docs.rs/zruntime/latest/zruntime/broadcast/index.html
[`mpmc`]: https://docs.rs/zruntime/latest/zruntime/mpmc/index.html
[the module's example]:
    https://docs.rs/zruntime/latest/zruntime/mpmc/index.html#spreading-work-over-threads
[`lock`]: https://docs.rs/zruntime/latest/zruntime/lock/index.html
[`unblock`]: https://docs.rs/zruntime/latest/zruntime/fn.unblock.html
[`Unblock`]: https://docs.rs/zruntime/latest/zruntime/struct.Unblock.html
[`fs`]: https://docs.rs/zruntime/latest/zruntime/fs/index.html
[`net`]: https://docs.rs/zruntime/latest/zruntime/net/index.html
[`process`]: https://docs.rs/zruntime/latest/zruntime/process/index.html
[`futures`]: https://docs.rs/futures
[`futures::future`]: https://docs.rs/futures/latest/futures/future/index.html
[`futures::stream`]: https://docs.rs/futures/latest/futures/stream/index.html
[`futures::io`]: https://docs.rs/futures/latest/futures/io/index.html
[`tracing`]: https://docs.rs/tracing
[zbus]: https://github.com/z-galaxy/zbus
[MIT]: (LICENSE)
