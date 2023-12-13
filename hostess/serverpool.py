from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import time
from typing import Hashable, Mapping, Optional, Sequence, Union

from hostess.subutils import RunCommand, Viewer


class ServerTask:
    """
    Wrapper for an asynchronously-executing task. It is primarily intended to
    be instantiated by `ServerPool` as an abstraction for a process running on
    a remote host.
    """

    def __init__(
        self,
        # TODO: some kind of type variable for this
        host: Union["Instance", RunCommand],
        method: str,
        args: Sequence,
        kwargs: Mapping,
        defer: bool = False,
        poll: float = 0.03,
    ):
        """
        Args:
            host: An object whose `method` attribute is a callable that returns
                a Viewer, most likely a `hostess.aws.ec2.Instance` or
                `hostess.ssh.SSH`.
            method: Name of a method of `host` that returns a `Viewer`.
            args: Args to pass to `host.method()`.
            kwargs: Kwargs to pass to `host.method()`.
            defer: If `True`, do not call `host.method()` on initialization.
            poll: Polling rate, in seconds, for `self.get()`.
        """
        self.instance, self.method = host, method
        self.args, self.kwargs = args, kwargs
        self.viewer, self.task = None, None
        self.executed, self.poll, self.exception = False, poll, None
        if defer is False:
            self.run()

    def get(self) -> Union[Viewer, Exception]:
        """
        Block until `self.viewer` is finished.

        Returns:
            A Viewer to a finished process, or an Exception if `host.method()`
                raised one.

        Raises:
            ValueError: If this object has not yet executed its task.
            TypeError: If task execution failed.
        """
        if self.viewer is None:
            if self.executed is False:
                raise ValueError("Task not yet executed.")
            raise TypeError(
                "Execution failed. Check self.exception for details."
            )
        self.viewer.wait()
        return self.viewer

    def run(self):
        """Execute the task."""
        self.executed = True
        try:
            viewer = getattr(
                self.instance, self.method
            )(*self.args, **self.kwargs)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            self.exception = ex
            raise ex
        if not isinstance(viewer, Viewer):
            self.exception = TypeError(
                f"host.method must return a Viewer; got {type(viewer)}."
            )
            raise self.exception
        self.viewer = viewer

    def kill(self):
        """Aliases self.viewer.kill(). Does nothing if self.viewer is None."""
        if self.viewer is not None:
            self.viewer.kill()

    @property
    def done(self) -> bool:
        """Aliases self.viewer.done. Always False if self.viewer is None."""
        return (self.viewer is not None) and self.viewer.done

    @property
    def running(self) -> bool:
        """Aliases self.viewer.running. Always False if self.viewer is None."""
        return (self.viewer is not None) and self.viewer.running

    @property
    def out(self) -> list:
        """Aliases self.viewer.out. Always [] if self.viewer is None."""
        return [] if self.viewer is None else self.viewer.out

    @property
    def err(self) -> list:
        """Aliases self.viewer.err. Always [] if self.viewer is None."""
        return [] if self.viewer is None else self.viewer.err


# TODO: optional/dontcare mode
class ServerPool:
    """
    Abstraction for a pool of asynchronous workers with hostess-compatible
    interfaces. Intended primarily for distributing tasks across remote hosts.
    Many alternatives are more appropriate for local tasks, such as the Python
    Standard Library's `concurrent.futures.ThreadPoolExecutor` and
    `multiprocessing.Pool`.
    """

    def __init__(
        self,
        hosts: Sequence[Union["Instance", RunCommand]],
        max_concurrent: int = 1,
        poll: float = 0.03,
    ):
        """
        Args:
            hosts: An object that has at least one method that returns a
                `Viewer`, most likely a `hostess.aws.ec2.Instance` or
                `hostess.ssh.SSH`.
            max_concurrent: Maximum number of tasks a single host may run
                concurrently. The maximum number of threads spawned by this
                object is thus `max_concurrent * len(hosts)`, +1 for its
                polling thread.
            poll: Polling rate, in seconds, for checking pending/running tasks.
        """
        idattr = None
        for identifier in ("instance_id", "ip", "host"):
            if all(hasattr(h, identifier) for h in hosts):
                idattr = identifier
                break
        if idattr is None:
            raise TypeError("These do not appear to be appropriate hosts.")
        self.max_concurrent = max_concurrent
        self.hosts = {getattr(h, idattr): h for h in hosts}
        self.taskmap = {getattr(h, idattr): {} for h in hosts}
        self.idattr = idattr
        self.pending, self.completed = {}, {}
        self.closed, self.terminated = False, False
        self.pollthread, self.exc = None, ThreadPoolExecutor(1)
        self.task_ix, self.poll = 0, poll
        self.used = set()

    def _rectify_call(self, kwargs):
        """
        helper function for `self.apply()`. don't allow new tasks when closed;
        don't let callers forbid `Viewers` or disown processes.

        Raises:
            ValueError: if pool is closed.
        """
        if self.closed is True:
            raise ValueError("pool closed")
        # TODO: check to make sure the called method returns a Viewer by
        #   default
        kwargs.pop('_viewer', None)
        kwargs.pop('_disown', None)

    @property
    def available(self) -> dict[Hashable, Union["Instance", RunCommand]]:
        """
        Available hosts.

        Returns:
            `dict` of {host id: host} containing only non-busy hosts.
        """
        return {
            i: self.hosts[i]
            for i, t in self.taskmap.items()
            if len(t) < self.max_concurrent
        }

    @property
    def next_available(
        self
    ) -> Optional[tuple[Hashable, Union["Instance", RunCommand]]]:
        """
        First available host, if any, preferring hosts that have not recently
        been assigned a task.

        Returns:
            `tuple` of (host id, host), if one is available; None otherwise.
        """
        if len((ready := self.available)) == 0:
            return None
        if len(ready) == 1:
            return list(ready.items())[0]
        if len(self.used) == len(self.taskmap):
            self.used = set()
        options = list((i, r) for i, r in ready.items())
        filtered = list(o for o in options if o[0] not in self.used)
        if len(filtered) == 0:
            return options[0]
        self.used.add(filtered[0][0])
        return filtered[0]

    @property
    def running(self) -> tuple[Viewer]:
        """
        Returns:
            All currently-running tasks.
        """
        tasks = [
            [t for t in v.values() if t.running] for v in self.taskmap.values()
        ]
        # noinspection PyTypeChecker
        return tuple(chain(*tasks))

    def __poll_loop(self):
        """Process poll loop. Should only be called by `self.__start()`."""
        while True:
            rcount = 0
            for iid, tasks in self.taskmap.items():
                for tix, task in tuple(tasks.items()):
                    if task.done:
                        self.completed[tix] = tasks.pop(tix).get()
                    elif self.terminated is True:
                        task.kill()
                    else:
                        rcount += 1
            # note that terminate() immediately sets pending to {}
            for tix in tuple(self.pending.keys()):
                if (id_host := self.next_available) is None:
                    continue
                self.taskmap[id_host[0]][
                    tix
                ] = ServerTask(id_host[1], *self.pending.pop(tix))
            if self.terminated is True or (
                (rcount + len(self.pending) == 0) and self.closed is True
            ):
                self.pollthread = None
                return
            time.sleep(self.poll)

    def __start(self):
        """
        Launch process polling loop, if necessary. Should only be called by
        `self.apply()`.
        """
        if self.pollthread is None:
            self.pollthread = self.exc.submit(self.__poll_loop)

    def apply(
        self,
        method: str,
        args: Sequence = (),
        kwargs: Mapping = None,
    ):
        """
        Submit a task to the host pool. Execute it immediately if a host is
        available; otherwise queue it for execution. Unlike some task pool
        methods of this type, `ServerPool.apply()` does not return a
        future-like object. The analogous object(s) can be found in
        `ServerPool.pending` when not yet executed, `ServerPool.taskmap` when
        running, and `ServerPool.completed` once done.

        Args:
            method: Name of method of host to call with `args` and `kwargs`.
                This method must return a `Viewer`.
            args: Args to pass to the named method.
            kwargs: kwargs to pass to the named method.
        """
        kwargs = {} if kwargs is None else kwargs
        self._rectify_call(kwargs)
        self.__start()
        if (id_host := self.next_available) is None:
            self.pending[self.task_ix] = (method, args, kwargs)
        else:
            self.taskmap[id_host[0]][
                self.task_ix
            ] = ServerTask(id_host[1], method, args, kwargs)
        self.task_ix += 1

    def __str__(self):
        n_running = len(self.running)
        if self.terminated:
            infix = " (terminated) "
        elif self.closed:
            infix = " (closed) "
        else:
            infix = ""
        return (
            f"ServerPool{infix}: {len(self.taskmap)} hosts, {n_running} "
            f"running, {len(self.pending)} pending, {len(self.completed)} "
            f"completed"
        )

    def __repr__(self):
        return self.__str__()

    def close(self):
        """Close the pool, preventing submission of new tasks."""
        self.closed = True

    def join(self):
        """Block until all pending and running tasks are complete."""
        while self.pollthread is not None:
            time.sleep(self.poll)

    def terminate(self):
        """
        Terminate the `ServerPool`. This will cancel all pending tasks, kill
        all running tasks, and prevent submission of new tasks.
        """
        self.pending = {}
        self.closed, self.terminated = True, True
        self.exc.shutdown()

    def gather(self) -> list[Viewer]:
        """
        Block until all pending and running tasks are complete, then terminate
        self, then return the results of all completed tasks in a list. Useful
        for 'under-the-hood' uses of `ServerPool`.

        Returns:
            A `list` of `Viewers` for completed tasks, in order of submission.
        """
        self.join()
        output = [self.completed[i] for i in sorted(self.completed.keys())]
        self.terminate()
        return output

    def __del__(self):
        self.close()
        self.join()
        self.exc.shutdown()
