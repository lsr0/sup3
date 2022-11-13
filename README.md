# sup3
[![Standalone Binaries](https://github.com/lsr0/sup3/actions/workflows/binaries.yml/badge.svg)](https://github.com/lsr0/sup3/actions/workflows/binaries.yml)


**A standalone S3 tool**

![Recording of download with progress enabled](/images/download_progress_anim_0.8.5.gif)

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
* [x] Upload arguments, e.g. ACLs
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
* [x] Make Bucket (`mb`) (S3 URIs)

## Speed

Meaurements performed on an i7-7700HQ, with an approximately 50ms RTT to the S3 server.

### Download - 20MiB file
| Command | Mean [s] | Min [s] | Max [s] | Relative | CPU User [s] | CPU System [s] | CPU Relative |
|:---|---:|---:|---:|---:|---:|---:|---:|
| `sup3 cp s3://bucket/file dir/` | 1.306 ± 0.047 | 1.249 | 1.416 | 1.00 | 0.095 | 0.153 | 100% |
| `aws s3 cp s3://bucket/file dir/` | 1.855 ± 0.106 | 1.724 | 2.044 | **142% ± 10%** | 0.565 | 0.122 | **277%** |

### Download - 224byte file
| Command | Mean [s] | Min [s] | Max [s] | Relative | CPU User [s] | CPU System [s] | CPU Relative |
|:---|---:|---:|---:|---:|---:|---:|---:|
| `sup3 cp s3://bucket/file dir/` | 0.246 ± 0.006 | 0.234 | 0.255 | 100% | 0.004 | 0.007 | 100% |
| `aws s3 cp s3://bucket/file dir/` | 0.910 ± 0.013 | 0.892 | 0.937 | **370% ± 11%** | 0.394 | 0.050 | **4036%** |

### ls - 63,186 files from bucket by substring
| Command | Mean [s] | Min [s] | Max [s] | Relative | CPU User [s] | CPU System [s] | CPU Relative |
|:---|---:|---:|---:|---:|---:|---:|---:|
| `sup3 ls --substring s3://bucket/substring >/dev/null` | 10.053 ± 0.694 | 9.253 | 10.487 | 100% | 0.485 | 0.141 | 100% |
| `aws s3 ls s3://bucket/substring >/dev/null` | 22.050 ± 3.809 | 19.643 | 26.441 | **219% ± 41%** | 11.772 | 0.143 | **1900%**

