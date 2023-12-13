from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import time
from typing import Union, Sequence, Mapping

from hostess.subutils import RunCommand, Viewer


class ServerTask:
    """
    pollable/deferrable wrapper for a subprocessed task execution. intended
    primarily for remote hosts.
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
        self.instance, self.method = host, method
        self.args, self.kwargs = args, kwargs
        self.viewer, self.task = None, None
        self.executed, self.poll = False, poll
        if defer is False:
            self.run()

    def get(self):
        if self.viewer is None:
            raise ValueError("Task not yet executed.")
        while self.viewer is None:
            time.sleep(self.poll)
            if (ex := self.task.exception()) is not None:
                return ex
        if self.viewer is not None:
            self.viewer.wait()
            return self.viewer
        raise TypeError("self.viewer not successfully set.")

    def run(self):
        self.executed = True
        viewer = getattr(self.instance, self.method)(*self.args, **self.kwargs)
        if not isinstance(viewer, Viewer):
            raise TypeError(
                f"Method must return Viewer, returned {type(viewer)} instead."
            )
        self.viewer = viewer

    def kill(self):
        if self.viewer is not None:
            self.viewer.kill()

    @property
    def done(self):
        return (self.viewer is not None) and self.viewer.done

    @property
    def running(self):
        return (self.viewer is not None) and self.viewer.running

    @property
    def out(self):
        return [] if self.viewer is None else self.viewer.out

    @property
    def err(self):
        return [] if self.viewer is None else self.viewer.err


class ServerPool:
    """
    class that maps tasks across arbitrary worker pools with hostess-compatible
    interfaces. Intended primarily for distributing tasks across groups of
    remote hosts.
    """

    def __init__(
        self,
        hosts: Sequence[Union["Instance", RunCommand]],
        max_concurrent: int = 1,
        poll: float = 0.03,
    ):
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
        if self.closed is True:
            raise ValueError("pool closed")
        # TODO: check to make sure the called method returns a Viewer by
        #   default
        kwargs.pop('_viewer', None)
        kwargs.pop('_disown', None)

    @property
    def available(self):
        return {
            i: self.hosts[i]
            for i, t in self.taskmap.items()
            if len(t) < self.max_concurrent
        }

    @property
    def next_available(self):
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
    def running(self):
        tasks = [
            [t for t in v.values() if t.running] for v in self.taskmap.values()
        ]
        return tuple(chain(*tasks))

    def __poll_loop(self):
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
        if self.pollthread is None:
            self.pollthread = self.exc.submit(self.__poll_loop)

    def apply(
        self,
        method: str,
        args: Sequence = (),
        kwargs: Mapping = None,
    ):
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
        self.closed = True

    def join(self):
        while self.pollthread is not None:
            time.sleep(self.poll)

    def terminate(self):
        self.pending = {}
        self.closed, self.terminated = True, True
        self.exc.shutdown()

    def gather(self):
        self.join()
        output = [self.completed[i] for i in sorted(self.completed.keys())]
        self.terminate()
        return output

    def __del__(self):
        self.close()
        self.join()
        self.exc.shutdown()
