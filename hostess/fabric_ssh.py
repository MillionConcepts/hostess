import io
import time
from multiprocessing import Process
from typing import Hashable, Mapping

from fabric import Connection

from hostess.config import GENERAL_DEFAULTS
from hostess.subutils import RunCommand


def open_tunnel(host, uname, keyfile, local_port, remote_port):
    """abstraction to open a tunnel with Fabric in a child process."""

    def target():
        conn = SSH.connect(host, uname, keyfile).conn
        try:
            with conn.forward_local(local_port, remote_port):
                while True:
                    time.sleep(1)
        except Exception as ex:
            return conn, ex

    process = Process(target=target)
    process.start()
    metadict = {
        'host': host,
        'uname': uname,
        'keyfile': keyfile,
        'local_port': local_port,
        'remote_port': remote_port,
    }
    # TODO: add a pipe or something
    return process, metadict


class SSH(RunCommand):
    """wrapper for managed command execution via fabric.Connection"""

    def __init__(
        self, command="", conn: Connection = None, key: str = None, **kwargs
    ):
        if conn is None:
            raise TypeError("a Connection must be provided")
        super().__init__(command, conn, conn['runners']['remote'], **kwargs)
        self.host, self.uname, self.key = conn.host, conn.user, key
        self.conn = conn  # effectively an alias for self.ctx
        self.tunnels = []

    @classmethod
    def connect(
        cls, host, uname=GENERAL_DEFAULTS['uname'], key=None
    ):
        connect_kwargs = {'key_filename': key} if key is not None else {}
        conn = Connection(user=uname, host=host, connect_kwargs=connect_kwargs)
        ssh = object().__new__(cls)
        ssh.__init__(conn=conn, key=key)
        return ssh

    def get(self, *args, **kwargs):
        return self.conn.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return self.conn.put(*args, **kwargs)

    def read_csv(self, fname, **csv_kwargs):
        import pandas as pd

        buffer = io.StringIO()
        buffer.write(self.get(fname).decode())
        buffer.seek(0)
        return pd.read_csv(buffer, **csv_kwargs)

    def tunnel(self, local_port, remote_port):
        process, meta = open_tunnel(
            self.host, self.uname, self.key, local_port, remote_port
        )
        self.tunnels.append((process, meta))

    def __str__(self):
        return f"{super().__str__()}\n{self.uname}@{self.host}"


# TODO: try fabric's pooled commands
def merge_csv(
    ssh_dict: Mapping[Hashable, SSH], fn: str, **csv_kwargs
) -> "pd.DataFrame":
    """
    merges csv files -- logs, perhaps -- from across a defined group
    of remote servers.
    """
    import pandas as pd

    framelist = []
    for name, ssh in ssh_dict.items():
        csv_df = ssh.read_csv(fn, **csv_kwargs)
        csv_df['server'] = name
        framelist.append(csv_df)
    return pd.concat(framelist).reset_index(drop=True)


