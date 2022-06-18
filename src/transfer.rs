use futures::{stream, future};

use stream::futures_unordered::FuturesUnordered;
use stream::StreamExt;
use futures::FutureExt;
use future::TryFutureExt;
use std::sync::Arc;

use crate::s3;
use crate::cli;
use super::MainResult;
use crate::shared_options::SharedOptions;

#[derive(clap::Args, Debug, Clone)]
pub struct OptionsTransfer {
    /// Perform multiple uploads concurrently
    #[clap(long, short='j', parse(try_from_str=flag_concurrency_in_range))]
    concurrency: Option<u16>,
    /// Continue to next file on error
    #[clap(long, short='y')]
    continue_on_error: bool,

    #[clap(flatten)]
    progress: cli::ArgProgress,
}

fn flag_concurrency_in_range(s: &str) -> Result<u16, String> {
    match s.parse() {
        Ok(val) if val > 0 => Ok(val),
        Ok(_) => Err("concurrency not at least 1".to_owned()),
        Err(e) => Err(format!("{e}")),
    }
}


pub async fn upload(local_paths: &[std::path::PathBuf], to: &s3::Uri, client: &s3::Client, opts: &SharedOptions, transfer: &OptionsTransfer) -> MainResult {
    let mut error_count = 0;
    let concurrency = transfer.concurrency.unwrap_or(1);

    let progress = cli::Output::new(&transfer.progress);

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

    for path in local_paths.iter() {
        let filename = path.file_name().as_ref().unwrap_or(&path.as_ref()).to_string_lossy().to_string();
        let update_fn = progress.add("initialising", filename);
        let update_fn_for_error = update_fn.clone();
        let fut = client.put(verbose, path, to, update_fn)
            .inspect_err(move |e| update_fn_for_error(cli::Update::Error(e.to_string())))
            .map_err(|e| (e, path.clone()))
            .inspect_ok(report_success);
        started_futures.push(fut);

        if started_futures.len() >= concurrency.into() {
            if let Err((e, path)) = started_futures.next().await.expect("at least one future") {
                report_error(&path, e);
                if !transfer.continue_on_error {
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

#[async_recursion::async_recursion]
async fn download_recursive_one(uri: s3::Uri, target: s3::Target, recursive: bool, progress: Arc<cli::Output>, client: s3::Client, verbose: bool, semaphore: Arc<tokio::sync::Semaphore>, options: OptionsTransfer) -> u32 {
    let token = semaphore.clone().acquire_owned().await.unwrap();
    let filename = uri.filename().unwrap_or(uri.key.as_str()).to_owned();
    let update_fn = progress.add("initialising", filename);
    let update_fn_for_error = update_fn.clone();
    let mut error_count = 0;
    let fut = client.get_recursive(verbose, recursive, uri.clone(), target.clone(), update_fn)
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
                let fut = download_recursive_one(s3::Uri::new(bucket.clone(), key), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), options.clone());
                handles.push(fut);
            }
            let results = futures::future::join_all(handles).await;
            error_count += results.into_iter().sum::<u32>();

        }
        Err(err) => {
            if !progress.progress_enabled() {
                progress.println_error(format_args!("failed to download {uri}: {err}"));
            }
            error_count += 1;
        }
    }
    error_count
}

pub async fn download(uris: &[s3::Uri], to: &std::path::PathBuf, client: &s3::Client, opts: &SharedOptions, transfer: &OptionsTransfer, recursive: bool) -> MainResult {
    let progress = Arc::new(cli::Output::new(&transfer.progress));
    let verbose = opts.verbose && !progress.progress_enabled();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(transfer.concurrency.unwrap_or(1) as usize));
    let cancellation = tokio_util::sync::CancellationToken::new();

    let ctrlc_cancel = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrlc_cancel.cancel();
    });

    let target = match s3::Target::new_create(uris, to) {
        Ok(i) => i,
        Err(err) => {
            progress.println_error(format_args!("{err}"));
            return MainResult::ErrorArguments;
        },
    };

    let mut handles = Vec::new();

    for uri in uris.iter() {
        let fut = download_recursive_one(uri.clone(), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), transfer.clone());
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
