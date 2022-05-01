# sup3

*A standalone S3 uploader*

## Warning
This is *pre-pre-alpha*

Anything and everything could go wrong. The [Rust AWS SDK](https://github.com/awslabs/aws-sdk-rust) used is also in developer preview.

## Usage goal
* CI and other minimal environments where pulling in python or building dependencies might be undesirable
* Eventually, as a consistent, reliable, fast and fun S3 tool for interactive use

## Features
* Cross-platform
* Uses authorisation support from the [Rust AWS SDK](https://github.com/awslabs/aws-sdk-rust)
* Streaming async uploads (files not read into memory)
* Upload resume on remote errors (provided by the SDK)
* No startup delay
* No runtime non-platform dependencies (e.g. `libc`, `libm`, `libgcc_s`)
* Concurrent uploads

## Commands
* Upload (`upload`) (`1..N` local files to S3 remote)
* Remove (`rm`) (a single S3 URI)
* List (`ls`) (`1..N` S3 URIs)

## TODO
* Directory upload support
* Optional progress reporting
* Support continuing to next file on error
* Binary size reduction
* More commands
