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
    Upload(Upload),
    /// Remove from S3
    ///
    /// Note: will succeed if remote file doesn't exist
    Rm(Remove),
    /// List S3 path
    Ls(ListFiles),
}

#[derive(Args, Debug)]
struct Upload {
    #[clap(required = true, parse(from_os_str))]
    paths: Vec<std::path::PathBuf>,
    /// S3 URI in s3://bucket/path/components format
    to: s3::Uri,
    /// Perform multiple uploads concurrently, will continue over errors
    #[clap(long, short='j', parse(try_from_str=flag_concurrency_in_range))]
    concurrency: Option<u16>,
    /// Continue to next file on error
    #[clap(long, short='y')]
    continue_on_error: bool,

    #[clap(flatten)]
    progress: cli::ArgProgress,
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
        let concurrency = self.concurrency.unwrap_or(1);

        let progress = cli::Output::new(&self.progress, self.paths.len());

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
        for (i, path) in self.paths.iter().enumerate() {
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
                    if !self.continue_on_error {
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
        Commands::Rm(remove) => remove.run(&client, &args.shared).await,
        Commands::Ls(list) => list.run(&client, &args.shared).await,
    }
}

