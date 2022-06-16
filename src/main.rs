mod s3;
mod shared_options;
mod cli;
mod transfer;

use clap::{Parser, Subcommand, Args};

use shared_options::SharedOptions;

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

#[derive(Args, Debug)]
struct Upload {
    #[clap(required = true, parse(from_os_str))]
    local_paths: Vec<std::path::PathBuf>,
    /// S3 URI in s3://bucket/path/components format
    to: s3::Uri,

    #[clap(flatten)]
    transfer: transfer::OptionsTransfer,
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
    transfer: transfer::OptionsTransfer,

    #[clap(long, short = 'r')]
    recursive: bool,
}

#[derive(Args, Debug)]
struct ListBuckets {
}

pub enum MainResult {
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

impl Upload {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        transfer::upload(&self.local_paths, &self.to, client, opts, &self.transfer).await
    }
}

impl Download {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        transfer::download(&self.uris, &self.to, client, opts, &self.transfer, self.recursive).await
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

