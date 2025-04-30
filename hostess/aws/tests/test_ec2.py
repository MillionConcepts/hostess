import math
import random
from functools import partial
import pytest

from hostess.aws.ec2 import Instance
# noinspection PyUnresolvedReferences
from hostess.aws.tests.aws_test_utils import (
    aws_cleanup_tasks,
    # NOTE: do not remove this import. pytest cannot find it for use in
    #  aws_cleanup_tasks() if not in explicit scope of this module.
    aws_fallback_cleanup,
    delete_keyfile,
    randstr,
    terminate_instance
)

HAVE_SSH = False

# NOTE: these fixtures are in part tests of hostess, and are a little sloppy to
#  implement as fixtures. There's nothing really _wrong_ with it, though,
#  and the code required to set up instances suitable for the subsequent
#  tests is complicated enough that it would basically involve reimplementing
#  large portions of hostess.aws.ec2 in this module, which is yet sloppier.
@pytest.fixture(scope="session")
def temp_instance_name(aws_cleanup_tasks):
    name = f"hostess-test-{randstr(10)}"
    instance = Instance.launch(
        options={
            "instance_type":"t3a.micro",
            "volume_size": 8,
            "volume_type": "gp2",
            "instance_name": name
        },
        verbose=False
    )
    aws_cleanup_tasks["ec2_instance"] = partial(
        terminate_instance, name
    )
    instance._maybe_find_key()
    aws_cleanup_tasks["delete_keyfile"] = partial(
        delete_keyfile, instance.key
    )

    yield name
    # normal, polite way of performing cleanup:
    for task in ("ec2_instance", "security_group", "keyfile"):
        if task in aws_cleanup_tasks.keys():
            aws_cleanup_tasks.pop(task)()


@pytest.fixture(scope="session")
def can_connect(temp_instance_name):
    """
    Partly a test of hostess, but mostly a check for the environment.
    Allows us to fail fast on tests that require SSH connections. If the
    instance successfully launches but this fails, it probably indicates
    something funny in network setup or conditions.
    """
    inst = Instance.find(name=temp_instance_name, verbose=False)
    try:
        inst.connect(maxtries=15)
    except Exception as ex:
        raise OSError(
            f"Instance not connectable: {type(ex)}: {ex}. Skipping tests "
            f"that require SSH connection."
        )


@pytest.fixture(scope="session")
def has_python(temp_instance_name, can_connect):
    inst = Instance.find(name=temp_instance_name, verbose=False)
    try:
        # noinspection PyArgumentList
        cproc = inst.install_conda()
        cproc.wait()
        assert cproc.returncode() == 0, f"exit code {cproc.returncode()}"
        assert inst.command(
            "ls miniforge3/bin/python", _wait=True
        ).out[0] == "miniforge3/bin/python", "python executable not installed"
        return
    except AssertionError:
        # TODO: report this better, although it's nontrivial -- unfortunately,
        #  the installer scripts write a bunch of things to stderr during
        #  normal operation
        error_msg = "remote processes failed"
    except Exception as ex:
        error_msg = f"{type(ex)}: {ex}"
    raise OSError(
        f"Couldn't install conda: {error_msg}. Skipping tests related "
        f"to remote Python calls."
    )


def test_command(temp_instance_name, can_connect):
    inst = Instance.find(name=temp_instance_name, verbose=False)
    assert inst.command("uname", _wait=True).out[0] == "Linux"


def test_call_python(temp_instance_name, can_connect, has_python):
    inst = Instance.find(name=temp_instance_name, verbose=False)
    ints = [random.randint(50, 1000) for _ in range(25)]
    proc = inst.call_python(
        "math",
        "lcm",
        ints,
        env="base",
        splat="*",
        serialization="pickle",
        compression="gzip",
        _wait=True
    )
    assert math.lcm(*ints) == int(proc.out[0])


def test_stop_start(temp_instance_name):
    inst = Instance.find(name=temp_instance_name, verbose=False)
    inst.stop()
    inst.wait_until_stopped(timeout=65)
    assert inst.state == "stopped", "instance failed to stop before timeout"
    inst.start()
    inst.wait_until_running(timeout=65)
    assert inst.state == "running", "instance failed to start before timeout"

