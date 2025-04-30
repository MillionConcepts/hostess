from hostess.aws.ec2 import Instance


# TODO, maybe: this is actually a test of hostess, and is
#  a little sloppy to implement as a fixture. There's nothing
#  really _wrong_ with it, though, and the alternative is to launch
#  an instance with plain boto3, set it up, use it for some tests,
#  terminate it, and then launch _another_ instance