mod s3;
mod shared_options;
mod cli;

use clap::{Parser, Subcommand, Args};
use futures::{stream, future};

use shared_options::SharedOptions;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Arguments {
    #[clap(subcommand)]
    command: Commands,

    #[clap(long, short='r')]
    region: Option<String>,

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
}

#[derive(Args, Debug)]
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
    remote_path: s3::Uri,
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
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        let mut error_count = 0;
        let concurrency = self.transfer.concurrency.unwrap_or(1);

        let progress = cli::Output::new(&self.transfer.progress, self.local_paths.len());

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
        for (i, path) in self.local_paths.iter().enumerate() {
            let filename = path.file_name().as_ref().unwrap_or(&path.as_ref()).to_string_lossy().to_string();
            let update_fn = progress.add(i, "initialising", filename);
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

        if error_count > 0 {
            std::process::exit(1);
        }
    }
}

impl Download {
    fn validate_arguments_create(&self) -> Result<bool, String> {
        match self.to.metadata() {
            Ok(meta) if meta.is_dir() => return Ok(true),
            Ok(_) if self.uris.len() > 1 => return Err("multiple uris and destination is not a directory".to_owned()),
            Ok(_) => return Ok(false),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&self.to).map_err(|e| e.to_string())?;
                return Ok(true);
            },
            Err(err) => return Err(err.to_string()),
        }
    }
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        let mut error_count = 0;
        let concurrency = self.transfer.concurrency.unwrap_or(1);

        let progress = cli::Output::new(&self.transfer.progress, self.uris.len());

        let mut report_error = |uri: &s3::Uri, e: &s3::Error| {
            if !progress.progress_enabled() {
                progress.println_error(format_args!("failed to download {uri}: {e}"));
            }
            error_count += 1;
        };

        let report_success = |path: &std::path::PathBuf| {
            if opts.verbose && concurrency > 0 && !progress.progress_enabled() {
                progress.println_done(format_args!("download {path:?}"));
            }
        };

        let verbose = opts.verbose && !progress.progress_enabled();

        let is_directory = match self.validate_arguments_create() {
            Ok(i) => i,
            Err(err) => {
                progress.println_error(format_args!("{err}"));
                std::process::exit(2);
            },
        };

        let target = match is_directory {
            true => s3::Target::Directory(self.to.clone()),
            false => s3::Target::File(self.to.clone()),
        };

        let mut started_futures = FuturesUnordered::new();
        for (i, uri) in self.uris.iter().enumerate() {
            let filename = uri.filename().unwrap_or(&uri.key).to_owned();
            let update_fn = progress.add(i, "initialising", filename);
            let update_fn_for_error = update_fn.clone();
            let fut = client.get(verbose, uri, &target, update_fn)
                .inspect_err(move |e| update_fn_for_error(cli::Update::Error(e.to_string())))
                .map_err(move |e| (e, uri.clone()))
                .inspect_ok(report_success);
            started_futures.push(fut);

            if started_futures.len() >= concurrency.into() {
                if let Err((e, uri)) = started_futures.next().await.expect("at least one future") {
                    report_error(&uri, &e);
                    if !self.transfer.continue_on_error {
                        break;
                    }
                    if e.should_cancel_other_operations() {
                        break;
                    }
                }
            }
        }
        while let Some(res) = started_futures.next().await {
            if let Err((e, path)) = res {
                report_error(&path, &e);
            }
        }

        if error_count > 0 {
            std::process::exit(1);
        }
    }
}

impl Remove {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        if let Err(e) = client.remove(opts, &self.remote_path).await {
            eprintln!("❌: failed to remove {:?}: {e}", self.remote_path);
            std::process::exit(1);
        }
    }
}

impl ListFiles {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        for uri in &self.remote_paths {
            if let Err(e) = client.ls(opts, &self.command_args, uri).await {
                eprintln!("❌: failed to list {:?}: {e}", uri);
                std::process::exit(1);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();

    let client = s3::init(args.region).await;

    match &args.command {
        Commands::Upload(upload) => upload.run(&client, &args.shared).await,
        Commands::Download(download) => download.run(&client, &args.shared).await,
        Commands::Rm(remove) => remove.run(&client, &args.shared).await,
        Commands::Ls(list) => list.run(&client, &args.shared).await,
    }
}

