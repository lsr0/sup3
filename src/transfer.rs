use futures::future;

use futures::FutureExt;
use future::TryFutureExt;
use std::sync::Arc;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

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

pub async fn upload(local_paths: &[std::path::PathBuf], to: &s3::Uri, client: &s3::Client, opts: &SharedOptions, transfer: &OptionsTransfer, recursive: bool) -> MainResult {
    let file_prefix = cli::longest_file_display_prefix(local_paths.iter().filter_map(|path| path.to_str()));
    let progress = Arc::new(cli::Output::new(&transfer.progress, opts.verbose, Some(file_prefix)));
    progress.add_incoming_tasks(local_paths.len());
    let semaphore = Arc::new(tokio::sync::Semaphore::new(transfer.concurrency.unwrap_or(1) as usize));

    let cancellation = tokio_util::sync::CancellationToken::new();
    let ctrlc_cancel = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrlc_cancel.cancel();
    });

    let verbose = opts.verbose && !progress.progress_enabled();

    let mut futures = FuturesUnordered::new();

    for path in local_paths.into_iter() {
        let fut = upload_recursive_one(path.to_owned(), to, recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), transfer.clone());
        futures.push(fut);

        if cancellation.is_cancelled() {
            break;
        }
    }

    let mut error_count = 0;
    loop {
        let result = tokio::select!{
            res = &mut futures.next() => res,
            _ = cancellation.cancelled() => {
                progress.mark_cancelled();
                return MainResult::Cancelled;
            },
        };
        match result {
            Some(count) => error_count += count,
            None => break,
        }
        if error_count > 0 && !transfer.continue_on_error {
            break;
        }
    }
    MainResult::from_error_count(error_count)
}

async fn upload_single(path: &std::path::PathBuf, to: &s3::Uri, progress: Arc<cli::Output>, update_fn: cli::ProgressFn, client: s3::Client, verbose: bool, _permit: tokio::sync::OwnedSemaphorePermit) -> u32 {
    let update_fn_for_error = update_fn.clone();
    match client.put(verbose, path, to, update_fn).await {
        Ok(uri) => {
            progress.println_done_verbose(format_args!("uploaded {uri}"));
            0
        },
        Err(e) => {
            progress.println_error_noprogress(format_args!("failed to upload {path:?} to {to}: {e}"));
            update_fn_for_error(cli::Update::Error(e.to_string()));
            1
        }
    }
}

#[async_recursion::async_recursion]
async fn upload_recursive_one(path: std::path::PathBuf, to: &s3::Uri, recursive: bool, progress: Arc<cli::Output>, client: s3::Client, verbose: bool, semaphore: Arc<tokio::sync::Semaphore>, options: OptionsTransfer) -> u32 {
    let token = semaphore.clone().acquire_owned().await.unwrap();

    let filename = path.to_string_lossy().to_string();
    let update_fn = progress.add("statting", filename);

    let metadata = match tokio::fs::metadata(&path).await {
        Ok(m) => m,
        Err(e) => {
            progress.println_error_noprogress(format_args!("failed to access local path {path:?}: {e}"));
            update_fn(cli::Update::Error(e.to_string()));
            return 1;
        },
    };

    if !metadata.is_dir() {
        return upload_single(&path, to, progress, update_fn, client, verbose, token).await;
    }
    if !recursive {
        progress.println_error_noprogress(format_args!("given directory {path:?} in non-recursive mode"));
        update_fn(cli::Update::Error("given directory in non-recursive mode".into()));
        return 1;
    }
    drop(token);
    update_fn(cli::Update::State("listing"));

    let extra_path_component = path.file_name().unwrap_or(std::ffi::OsStr::new(""));
    let extra_path_component_utf = match extra_path_component.to_str() {
        None => {
            progress.println_error_noprogress(format_args!("directory child not unicode {extra_path_component:?}"));
            update_fn(cli::Update::Error(format!("directory child not unicode {extra_path_component:?}")));
            return 1;
        },
        Some(p) => p,
    };

    let to_child = to.child_directory(extra_path_component_utf);

    let mut files = match tokio::fs::read_dir(path).await {
        Err(e) => { update_fn(cli::Update::Error(e.to_string())); return 1; },
        Ok(files) => files,
    };

    let mut futures = FuturesUnordered::new();
    let mut error_count = 0;
    loop {
        let child_file = match files.next_entry().await {
            Err(e) => {
                progress.println_error_noprogress(format_args!("failed to list directory: {e}"));
                update_fn(cli::Update::Error(e.to_string()));
                // Run all other already pushed futures to completion
                if !options.continue_on_error {
                    return 1;
                }
                error_count += 1;
                break;
            },
            Ok(Some(file)) => file,
            Ok(None) => break,
        };
        progress.add_incoming_tasks(1);

        futures.push(upload_recursive_one(child_file.path(), &to_child, recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), options.clone()));
    }

    update_fn(cli::Update::FinishedHide());
    while let Some(res) = futures.next().await {
        error_count += res;
        if error_count > 0 && !options.continue_on_error {
            return error_count;
        }
    }
    error_count
}

#[async_recursion::async_recursion]
async fn download_recursive_one(uri: s3::Uri, target: s3::Target, recursive: bool, progress: Arc<cli::Output>, client: s3::Client, verbose: bool, semaphore: Arc<tokio::sync::Semaphore>, options: OptionsTransfer, create_directory: bool) -> u32 {
    let token = semaphore.clone().acquire_owned().await.unwrap();
    let update_fn = progress.add("initialising", uri.to_string());
    let update_fn_for_error = update_fn.clone();
    let mut error_count = 0;
    use std::io::ErrorKind::AlreadyExists;
    if create_directory {
        let create_result = tokio::fs::create_dir(&target.path()).await
            .or_else(|err| if err.kind() == AlreadyExists { Ok(()) } else { Err(err) });
        if let Err(e) = create_result {
            update_fn_for_error(cli::Update::Error(format!("creating directory: {e}")));
            progress.println_error_noprogress(format_args!("creating directory: {e}"));
        }
    }
    let fut = client.get_recursive(verbose, recursive, uri.clone(), target.clone(), update_fn)
        .inspect_err(move |e| update_fn_for_error(cli::Update::Error(e.to_string())))
        .map(|res| (res, token));
    let (res, ..) = fut.await;
    match res {
        Ok(s3::GetRecursiveResult::One(path)) => if verbose && options.concurrency.unwrap_or(1) > 1 && !progress.progress_enabled() {
            progress.println_done_verbose(format_args!("downloaded {path:?}"));
        },
        Ok(s3::GetRecursiveResult::Many{bucket, keys, target}) => {
            // Create directories for all children, recursive file results here.
            let mut futures = FuturesUnordered::new();
            progress.add_incoming_tasks(keys.len());
            for key in keys {
                let additional_path = &key[uri.key.len()..];
                let additional_dir = additional_path.rsplit_once('/').map(|(dir, _filename)| dir);
                let target = match additional_dir {
                    Some(dir) => target.child(dir),
                    None => target.clone(),
                };
                let create_dir = additional_dir.is_some();
                let key = key.clone();
                let fut = download_recursive_one(s3::Uri::new(bucket.clone(), key), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), options.clone(), create_dir);
                futures.push(fut);
            }
            while let Some(res) = futures.next().await {
                error_count += res;
                if error_count > 0 && !options.continue_on_error {
                    return error_count;
                }
            }

        }
        Err(err) => {
            progress.println_error_noprogress(format_args!("failed to download {uri}: {err}"));
            error_count += 1;
        }
    }
    error_count
}

pub async fn download(uris: &[s3::Uri], to: &std::path::PathBuf, client: &s3::Client, opts: &SharedOptions, transfer: &OptionsTransfer, recursive: bool) -> MainResult {
    let uri_prefix = cli::longest_file_display_prefix(uris.iter().map(|uri| uri.to_string()));
    let progress = Arc::new(cli::Output::new(&transfer.progress, opts.verbose, Some(uri_prefix.clone())));
    progress.add_incoming_tasks(uris.len());
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
            progress.println_error(format_args!("local path {to:?}: {err}"));
            return MainResult::ErrorArguments;
        },
    };

    let mut futures = FuturesUnordered::new();

    for uri in uris.iter() {
        let fut = download_recursive_one(uri.clone(), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), transfer.clone(), false);
        futures.push(fut);

        if cancellation.is_cancelled() {
            break;
        }
    }

    let mut error_count = 0;
    loop {
        let result = tokio::select!{
            res = &mut futures.next() => res,
            _ = cancellation.cancelled() => {
                progress.mark_cancelled();
                return MainResult::Cancelled;
            },
        };
        match result {
            Some(count) => error_count += count,
            None => break,
        }
        if error_count > 0 && !transfer.continue_on_error {
            break;
        }
    }
    MainResult::from_error_count(error_count)
}
