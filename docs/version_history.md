# Version History

## [0.9.0] - 2023-12-15
### Added

- whole-`Cluster` file upload/download methods (`read()`, `get()`, `put()`, 
`read_csv()`)
- can now set default S3 `TransferConfig` in `hostess.config`
- `RefAlarm` tool for debugging reference leaks / drops
- `Cluster` and `Instance` now have `install_conda()` methods that perform 
managed installation(s) of a Conda distribution of Python (by default miniforge)
- `ServerPool` object for managed asynchronous dispatch of arbitrary numbers 
of tasks to remote hosts

### Changed
- `Cluster` can now be indexed by `Instance` names, instance ids, and ip 
addresses as well as their indices within the `Cluster`'s `instances` list
- `Cluster` wait-until-connected behavior is now available as an individual 
method (`connect()`), not just from `launch()`
- various `Cluster` methods can now be run in 'permissive' mode, returning 
exceptions rather than raising them (intended to allow instructions to execute 
on some instances even if attempts to execute instructions on others fail)
- backed by `ServerPool`, `Cluster`'s `commandmap()` and `pythonmap()` now 
accept arbitrary numbers of tasks, dispatching them to instances as soon as they free up
- `hostess.caller.generic_python_endpoint()` now permits returning objects in 
pickled and/or gzipped form (preparation for high-level features in a later version)
- `dicts` returned by the `price_per_hour()` methods of `Cluster` and `Instance`
now order keys the same way
- assorted updates to documentation and examples

### Fixed
- `Bucket.put_stream()` updated to work with new `Bucket` internals
- `console_stream_handlers()` no longer creates secret duplicate stdout/stderr
caches
- edge case in `find_conda_env()` that sometimes prevented finding the most 
recently-created env
- edge case in caller json serialization for some nested objects containing 
strings
- `Instance.conda_env()` now autoconnects as intended

### Removed
- Because they now accept arbitrary numbers of tasks, `Cluster.commandmap()` 
and `pythonmap()` no longer interpret sequences of bare strings passed to 
`argseq` as individual tasks (due to both ambiguity and partial incompatibility
with `pythonmap()'s` signature). They will always interpret a sequence of bare 
strings as a `tuple` of args to be included in all tasks.

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
- bug with packing ndarrays of homogeneous dtype

## [0.7.3] - 2023-11-28

First conda-forge version; start of changelog.
