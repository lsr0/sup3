mod s3;
mod shared_options;
mod cli;

use clap::{Parser, Subcommand, Args};
use futures::{stream, future};

use shared_options::SharedOptions;
use futures::FutureExt;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Arguments {
    #[clap(subcommand)]
    command: Commands,

    #[clap(long, short='R', global=true)]
    region: Option<String>,

    #[clap(long, short='e', global=true)]
    /// Use custom endpoint URL for other S3 implementations
    endpoint: Option<http::uri::Uri>,

    #[clap(flatten)]
    shared: SharedOptions,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Upload to S3
    #[clap(alias="up")]
    Upload(Upload),
    /// Download from S3
    #[clap(alias="down")]
    Download(Download),
    /// Remove from S3
    ///
    /// Note: will succeed if remote file doesn't exist
    Rm(Remove),
    /// List S3 path
    Ls(ListFiles),
    /// List S3 buckets
    #[clap(alias="lb")]
    ListBuckets(ListBuckets),
}

#[derive(Args, Debug, Clone)]
struct OptionsTransfer {
    /// Perform multiple uploads concurrently
    #[clap(long, short='j', parse(try_from_str=flag_concurrency_in_range))]
    concurrency: Option<u16>,
    /// Continue to next file on error
    #[clap(long, short='y')]
    continue_on_error: bool,

    #[clap(flatten)]
    progress: cli::ArgProgress,
}

#[derive(Args, Debug)]
struct Upload {
    #[clap(required = true, parse(from_os_str))]
    local_paths: Vec<std::path::PathBuf>,
    /// S3 URI in s3://bucket/path/components format
    to: s3::Uri,

    #[clap(flatten)]
    transfer: OptionsTransfer,
}

#[derive(Args, Debug)]
struct Remove {
    /// S3 URI in s3://bucket/path/components format
    #[clap(required = true)]
    remote_paths: Vec<s3::Uri>,
}

#[derive(Args, Debug)]
struct ListFiles {
    /// S3 URIs in s3://bucket/path/components format
    #[clap(required = true)]
    remote_paths: Vec<s3::Uri>,
    #[clap(flatten)]
    command_args: s3::ListArguments,
}

#[derive(Args, Debug)]
struct Download {
    /// S3 URIs in s3://bucket/path/components format
    #[clap(required = true)]
    uris: Vec<s3::Uri>,
    #[clap(parse(from_os_str))]
    to: std::path::PathBuf,

    #[clap(flatten)]
    transfer: OptionsTransfer,

    #[clap(long, short = 'r')]
    recursive: bool,
}

#[derive(Args, Debug)]
struct ListBuckets {
}

enum MainResult {
    Success,
    ErrorArguments,
    ErrorSomeOperationsFailed,
    Cancelled,
}

impl MainResult {
    pub fn from_error_count(count: u32) -> MainResult {
        match count {
            0 => MainResult::Success,
            _ => MainResult::ErrorSomeOperationsFailed,
        }
    }
}

impl std::process::Termination for MainResult {
    fn report(self) -> std::process::ExitCode {
        match self {
            Self::Success => return std::process::ExitCode::SUCCESS,
            Self::ErrorArguments => return std::process::ExitCode::from(1),
            Self::ErrorSomeOperationsFailed => return std::process::ExitCode::from(2),
            Self::Cancelled => return std::process::ExitCode::from(3),
        }
    }
}

fn flag_concurrency_in_range(s: &str) -> Result<u16, String> {
    match s.parse() {
        Ok(val) if val > 0 => Ok(val),
        Ok(_) => Err("concurrency not at least 1".to_owned()),
        Err(e) => Err(format!("{e}")),
    }
}

use stream::futures_unordered::FuturesUnordered;
use stream::StreamExt;
use future::TryFutureExt;

impl Upload {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        let mut error_count = 0;
        let concurrency = self.transfer.concurrency.unwrap_or(1);

        let progress = cli::Output::new(&self.transfer.progress);

        let mut report_error = |path: &std::path::Path, e| {
            if !progress.progress_enabled() {
                progress.println_error(format_args!("failed to upload {path:?}: {e}"));
            }
            error_count += 1;
        };

        let report_success = |uri: &String| {
            if opts.verbose && concurrency > 0 && !progress.progress_enabled() {
                progress.println_done(format_args!("uploaded {uri}"));
            }
        };

        let verbose = opts.verbose && !progress.progress_enabled();

        let mut started_futures = FuturesUnordered::new();

        for path in self.local_paths.iter() {
            let filename = path.file_name().as_ref().unwrap_or(&path.as_ref()).to_string_lossy().to_string();
            let update_fn = progress.add("initialising", filename);
            let update_fn_for_error = update_fn.clone();
            let fut = client.put(verbose, path, &self.to, update_fn)
                .inspect_err(move |e| update_fn_for_error(cli::Update::Error(e.to_string())))
                .map_err(|e| (e, path.clone()))
                .inspect_ok(report_success);
            started_futures.push(fut);

            if started_futures.len() >= concurrency.into() {
                if let Err((e, path)) = started_futures.next().await.expect("at least one future") {
                    report_error(&path, e);
                    if !self.transfer.continue_on_error {
                        break;
                    }
                }
            }
        }
        while let Some(res) = started_futures.next().await {
            if let Err((e, path)) = res {
                report_error(&path, e);
            }
        }

        MainResult::from_error_count(error_count)
    }
}

#[async_recursion::async_recursion]
async fn start_one(uri: s3::Uri, target: s3::Target, recursive: bool, progress: Arc<cli::Output>, client: s3::Client, verbose: bool, semaphore: Arc<tokio::sync::Semaphore>, options: OptionsTransfer, cancel: tokio_util::sync::CancellationToken) -> u32 {
    if cancel.is_cancelled() {
        return 1;
    }
    let token = semaphore.clone().acquire_owned().await.unwrap();
    let filename = uri.filename().unwrap_or(uri.key.as_str()).to_owned();
    let update_fn = progress.add("initialising", filename);
    let update_fn_for_error = update_fn.clone();
    let mut error_count = 0;
    let fut = client.get_recursive(verbose, recursive, uri.clone(), target.clone(), update_fn, cancel.clone())
        .inspect_err(move |e| update_fn_for_error(cli::Update::Error(e.to_string())))
        .map(|res| (res, token));
    let (res, ..) = fut.await;
    match res {
        Ok(s3::GetRecursiveResult::One(path)) => if verbose && options.concurrency.unwrap_or(1) > 1 && !progress.progress_enabled() {
            progress.println_done(format_args!("download {path:?}"));
        },
        Ok(s3::GetRecursiveResult::Many{bucket, keys, target}) => {
            let mut handles = Vec::new();
            for key in keys {
                let key = key.clone();
                let fut = start_one(s3::Uri::new(bucket.clone(), key), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), options.clone(), cancel.clone());
                handles.push(fut);
            }
            let results = futures::future::join_all(handles).await;
            error_count += results.into_iter().sum::<u32>();

        }
        Err(err) => {
            if !progress.progress_enabled() {
                progress.println_error(format_args!("failed to download {uri}: {err}"));
            }
            if !options.continue_on_error || err.should_cancel_other_operations() {
                cancel.cancel();
            }
            error_count += 1;
        }
    }
    error_count
}

impl Download {
    fn validate_arguments_create(&self) -> Result<s3::Target, String> {
        match self.to.metadata() {
            Ok(meta) if meta.is_dir() => Ok(s3::Target::Directory(self.to.clone())),
            Ok(_) if self.uris.len() > 1 => Err("multiple uris and destination is not a directory".to_owned()),
            Ok(_) => Ok(s3::Target::File(self.to.clone())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&self.to).map_err(|e| e.to_string())?;
                Ok(s3::Target::Directory(self.to.clone()))
            },
            Err(err) => Err(err.to_string()),
        }
    }
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        let progress = Arc::new(cli::Output::new(&self.transfer.progress));
        let verbose = opts.verbose && !progress.progress_enabled();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.transfer.concurrency.unwrap_or(1) as usize));
        let cancellation = tokio_util::sync::CancellationToken::new();

        let ctrlc_cancel = cancellation.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            ctrlc_cancel.cancel();
        });

        let target = match self.validate_arguments_create() {
            Ok(i) => i,
            Err(err) => {
                progress.println_error(format_args!("{err}"));
                return MainResult::ErrorArguments;
            },
        };

        let mut handles = Vec::new();

        for uri in self.uris.iter() {
            let fut = start_one(uri.clone(), target.clone(), self.recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), self.transfer.clone(), cancellation.clone());
            handles.push(fut);

            if cancellation.is_cancelled() {
                break;
            }
        }
        let mut wait_all = futures::future::join_all(handles);
        let results = tokio::select!{
            res = &mut wait_all => res,
            _ = cancellation.cancelled() => {
                progress.mark_cancelled();
                return MainResult::Cancelled;
            },
        };
        let error_count = results.into_iter().sum();
        MainResult::from_error_count(error_count)
    }
}

impl Remove {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        for uri in &self.remote_paths {
            if let Err(e) = client.remove(opts, &uri).await {
                eprintln!("❌: failed to remove {}: {e}", uri);
                return MainResult::ErrorSomeOperationsFailed;
            }
        }
        MainResult::Success
    }
}

impl ListFiles {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        for uri in &self.remote_paths {
            if let Err(e) = client.ls(opts, &self.command_args, uri).await {
                eprintln!("❌: failed to list {uri}: {e}");
                return MainResult::ErrorSomeOperationsFailed;
            }
        }
        MainResult::Success
    }
}

impl ListBuckets {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        if let Err(e) = client.list_buckets(opts).await {
            eprintln!("❌: failed to list buckets: {e}");
            return MainResult::ErrorSomeOperationsFailed;
        }
        MainResult::Success
    }
}

#[tokio::main]
async fn main() -> MainResult {
    let args = Arguments::parse();

    let client = s3::init(args.region, args.endpoint).await;

    let exit_code = match &args.command {
        Commands::Upload(upload) => upload.run(&client, &args.shared).await,
        Commands::Download(download) => download.run(&client, &args.shared).await,
        Commands::Rm(remove) => remove.run(&client, &args.shared).await,
        Commands::Ls(list) => list.run(&client, &args.shared).await,
        Commands::ListBuckets(list_buckets) => list_buckets.run(&client, &args.shared).await,
    };
    exit_code
}

