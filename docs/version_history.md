# Version History
## [0.8.0] - 2023-12-07
### Added
- dispatched asynchronous I/O for `aws.s3.Bucket`
- variable-argument "mapped" versions of `call_python()` and `command()` for 
  `aws.ec2.Cluster`
- read-to-memory functionality for SSH-backed objects and `Bucket`
- dynamic price-per-hour calculation for EC2 instances
- tutorial Notebooks for `aws.s3` and `aws.ec2`
- experimental extended return options for `caller`-generated functions

### Changed
- most module-level plain functions in `aws.s3` are now methods of `Bucket`
- performance, stability, and consistency improvements to `Bucket.ls()`
- SSH tunnels now run in threads instead of subprocesses
- additional options for `aws.utilities.autopage()`
- small changes to `caller.generic_python_endpoint()` signature
- updated documentation
- EC2 wait-until-state-reached methods now accept timeouts
- `Cluster.launch()` now includes a block-until-connected option 
(`connect=True`) along with its existing block-until-started option 
(`wait=True`) 

### Fixed
- assorted MacOS compatibility issues
- several AWS pricing API regressions
- bug in `Bucket.put()`
- bug with passing `int`/`float` positional arguments to `subutils.RunCommand` 
and subclasses

## [0.7.3] - 2023-11-28

First conda-forge version; start of changelog.
