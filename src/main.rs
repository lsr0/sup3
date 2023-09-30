#![doc = include_str!("../README.md")]
mod arguments;
mod s3;
mod shared_options;
mod cli;
mod transfer;

use arguments::MainResult;
use arguments::Commands;
use clap::Parser;

#[tokio::main]
async fn main() -> MainResult {
    let args = arguments::Arguments::parse();

    let client = s3::init(args.region, args.endpoint, args.profile.as_deref()).await;

    let exit_code = match &args.command {
        Commands::Upload(upload) => upload.run(&client, &args.shared).await,
        Commands::Download(download) => download.run(&client, &args.shared).await,
        Commands::Rm(remove) => remove.run(&client, &args.shared).await,
        Commands::Ls(list) => list.run(&client, &args.shared).await,
        Commands::ListBuckets(list_buckets) => list_buckets.run(&client, &args.shared).await,
        Commands::Cp(copy) => copy.run(&client, &args.shared).await,
        Commands::Cat(cat) => cat.run(&client, &args.shared).await,
        Commands::MakeBuckets(make_buckets) => make_buckets.run(&client, &args.shared).await,
        #[cfg(feature = "gen-completion")]
        Commands::GenerateCompletion(cmd) => cmd.run(&client, &args.shared).await,
    };
    exit_code
}

