mod s3;
mod shared_options;

use clap::{Parser, Subcommand, Args};

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

impl Upload {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        for path in &self.paths {
            match client.put(opts, path, &self.to).await {
                Ok(()) => {},
                Err(e) => {
                    eprintln!("failed to upload {path:?}: {e}");
                    std::process::exit(1);
                },
            }
        }
    }
}

impl Remove {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        match client.remove(opts, &self.remote_path).await {
            Ok(()) => {},
            Err(e) => {
                eprintln!("failed to remove {:?}: {e}", self.remote_path);
                std::process::exit(1);
            },
        }
    }
}

impl ListFiles {
    async fn run(&self, client: &s3::Client, opts: &SharedOptions) {
        for uri in &self.remote_paths {
            match client.ls(opts, &self.command_args, uri).await {
                Ok(()) => {},
                Err(e) => {
                    eprintln!("failed to list {:?}: {e}", uri);
                    std::process::exit(1);
                },
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

