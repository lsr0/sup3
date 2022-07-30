# sup3

**A standalone S3 tool**

## Warning
This is **alpha**

Anything and everything could go wrong. The [Rust AWS SDK](https://github.com/awslabs/aws-sdk-rust) used is also in developer preview.

## Usage goal
* CI and other minimal environments where pulling in python or building dependencies might be undesirable
* Eventually, as a consistent, reliable, fast and fun S3 tool for interactive use


## Design
* Act as much as possible like existing unix commands (e.g. cp, ls, cat)
* Progress reporting by default where it makes sense (e.g. cp, down, up)


## Features
* Cross-platform
* Uses authorisation support from the [Rust AWS SDK](https://github.com/awslabs/aws-sdk-rust)
* Streaming async transfers (files not read into memory)
* Upload resume on remote errors (provided by the SDK)
* No startup delay
* No runtime non-platform dependencies (e.g. `libc`, `libm`, `libgcc_s`)
* [x] Concurrent transfers
* [x] Optional progress reporting
* [x] Recursive upload support
* [x] Recursive download support
* [ ] Remote globbing (e.g. `sup3 ls s3://bucket/media/**/highres*.png .`)
* [ ] Binary size reduction
* [ ] Upload arguments, e.g. ACLs
* [ ] Config file support
* [ ] List only files or only directories
* [x] List paging
* [ ] Server to server copy
* [x] Custom endpoints for other S3-compatible hosts (`--endpoint`)


## Commands
* Upload (`upload`|`down`) (local files to S3 remote)
* Download (`download`|`down`) (S3 URIs to local file/directory)
* Remove (`rm`) (S3 URIs)
* List (`ls`) (`1..N` S3 URIs)
* [x] List Buckets (`list-buckets`|`lb`)
* [x] Copy (`cp`)
* [x] Cat (S3 URIs)
* [ ] Touch (S3 URIs)

