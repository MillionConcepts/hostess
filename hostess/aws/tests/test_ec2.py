import pytest

from hostess.aws.ec2 import Instance
from hostess.aws.tests.aws_test_utils import randstr, terminate_instance
from hostess.utilities import curry


# NOTE: this is actually a test of hostess, and is a little sloppy to
#  implement in a fixture. There's nothing really _wrong_ with it, though,
#  and the code required to set up instances suitable for the subsequent
#  tests is complicated enough that it would basically involve reimplementing
#  large portions of hostess.aws.ec2 in this test module, which is even
#  sloppier.

@pytest.fixture(scope="session")
def temp_instance_info(aws_cleanup_tasks):
    name = f"hostess-test-{randstr(10)}"
    instance = Instance.launch(
        instance_type="t3a.micro",
        volume_size=8,
        volume_type='gp2',
        instance_name=name
    )
    aws_cleanup_tasks["ec2_instance"] = curry(
        terminate_instance, instance.ip
    )
    yield {'name': name, 'ip': instance.ip}
    # normal, polite way of performing cleanup:
    if "ec2_instance" in aws_cleanup_tasks.keys():
        aws_cleanup_tasks.pop("ec2_instance")()

