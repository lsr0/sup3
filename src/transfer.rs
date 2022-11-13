use futures::FutureExt;
use std::sync::Arc;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::num::NonZeroU16;

use crate::s3;
use crate::cli;
use super::MainResult;
use crate::shared_options::SharedOptions;

#[derive(clap::Args, Debug, Clone)]
pub struct OptionsTransfer {
    /// Perform multiple uploads concurrently
    #[clap(long, short='j', default_value="1")]
    concurrency: NonZeroU16,
    /// Continue to next file on error
    #[clap(long, short='y')]
    continue_on_error: bool,

    #[clap(flatten)]
    progress: cli::ArgProgress,
}

pub async fn upload(local_paths: &[std::path::PathBuf], to: &s3::Uri, client: &s3::Client, opts: &SharedOptions, transfer: &OptionsTransfer, opts_upload: &s3::OptionsUpload, recursive: bool) -> MainResult {
    let file_prefix = cli::longest_file_display_prefix(local_paths.iter().filter_map(|path| path.to_str()));
    let progress = Arc::new(cli::Output::new(&transfer.progress, opts.verbose, Some(file_prefix)));
    progress.add_incoming_tasks(local_paths.len());
    let semaphore = Arc::new(tokio::sync::Semaphore::new(transfer.concurrency.get() as usize));

    let cancellation = tokio_util::sync::CancellationToken::new();
    let ctrlc_cancel = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrlc_cancel.cancel();
    });

    let verbose = opts.verbose && !progress.progress_enabled();

    let mut futures = FuturesUnordered::new();

    for path in local_paths.into_iter() {
        let fut = upload_recursive_one(path.to_owned(), to, recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), transfer.clone(), opts_upload);
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

async fn upload_single(path: &std::path::PathBuf, to: &s3::Uri, progress: Arc<cli::Output>, update_fn: cli::ProgressFn, client: s3::Client, verbose: bool, opts_upload: &s3::OptionsUpload, _permit: tokio::sync::OwnedSemaphorePermit) -> u32 {
    let update_fn_for_error = update_fn.clone();
    match client.put(verbose, opts_upload, path, to, update_fn).await {
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
async fn upload_recursive_one(path: std::path::PathBuf, to: &s3::Uri, recursive: bool, progress: Arc<cli::Output>, client: s3::Client, verbose: bool, semaphore: Arc<tokio::sync::Semaphore>, options: OptionsTransfer, opts_upload: &s3::OptionsUpload) -> u32 {
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
        return upload_single(&path, to, progress, update_fn, client, verbose, opts_upload, token).await;
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

        futures.push(upload_recursive_one(child_file.path(), &to_child, recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), options.clone(), opts_upload));
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
async fn download_recursive_one(uri: s3::Uri, target: s3::Target, recursive: bool, progress: Arc<cli::Output>, client: s3::Client, verbose: bool, semaphore: Arc<tokio::sync::Semaphore>, options: OptionsTransfer) -> u32 {
    let token = semaphore.clone().acquire_owned().await.unwrap();
    let update_fn = progress.add("initialising", uri.to_string());
    let update_fn_for_error = update_fn.clone();
    let mut error_count = 0;
    let (res, ..) = client.get_recursive_stream(verbose, recursive, uri.clone(), target.clone(), update_fn)
        .map(|res| (res, token))
        .await;
    match res {
        Ok(s3::GetRecursiveResultStream::One(path)) => if verbose && options.concurrency.get() > 1 && !progress.progress_enabled() {
            progress.println_done_verbose(format_args!("downloaded {path:?}"));
        },
        Ok(s3::GetRecursiveResultStream::Many(mut list_stream)) => {
            let stream = list_stream.stream();
            futures::pin_mut!(stream);
            while let Some(res) = stream.next().await {
                let page = match res {
                    Ok(p) => p,
                    Err(e) => {
                        error_count += 1;
                        update_fn_for_error(cli::Update::Error(format!("fetching list files page: {e}")));
                        progress.println_error_noprogress(format_args!("fetching list files page: {e}"));
                        break;
                    },
                };
                let mut futures = FuturesUnordered::new();
                let file_count = page.iter().filter(|e| matches!(e, s3::RecursiveStreamItem::File(_))).count();
                progress.add_incoming_tasks(file_count);
                for entry in page {
                    match entry {
                        s3::RecursiveStreamItem::Directory(key) => {
                            let additional_dir: &str = &key[uri.key.len()..];
                            if additional_dir.len() > 0 {
                                let mut path = target.path();
                                path.push(additional_dir);
                                use std::io::ErrorKind::AlreadyExists;
                                let create_result = tokio::fs::create_dir(&path).await
                                    .or_else(|err| if err.kind() == AlreadyExists { Ok(()) } else { Err(err) });
                                if let Err(e) = create_result {
                                    progress.println_error_noprogress(format_args!("creating directory {path:?}: {e}"));
                                    let dir_update_fn = progress.add("creating directory", additional_dir.to_string());
                                    dir_update_fn(cli::Update::Error(format!("creating dir: {e}")));
                                    if !options.continue_on_error {
                                        return error_count + 1;
                                    }
                                }
                            }
                        },
                        s3::RecursiveStreamItem::File(key) => {
                            let additional_path: &str = &key[uri.key.len()..];
                            let additional_dir = additional_path.rsplit_once('/').map(|(dir, _filename)| dir);
                            let target = match additional_dir {
                                Some(dir) => target.child(dir),
                                None => target.clone(),
                            };
                            let fut = download_recursive_one(s3::Uri::new(uri.bucket.clone(), key), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), options.clone());
                            futures.push(fut);
                        },
                    };
                }
                while let Some(res) = futures.next().await {
                    error_count += res;
                    if error_count > 0 && !options.continue_on_error {
                        return error_count;
                    }
                }
            }
        }
        Err(err) => {
            update_fn_for_error(cli::Update::Error(err.to_string()));
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

    let semaphore = Arc::new(tokio::sync::Semaphore::new(transfer.concurrency.get() as usize));
    let cancellation = tokio_util::sync::CancellationToken::new();

    let ctrlc_cancel = cancellation.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrlc_cancel.cancel();
    });

    let target = match s3::Target::new_create(uris, to, true) {
        Ok(i) => i,
        Err(err) => {
            progress.println_error(format_args!("local path {to:?}: {err}"));
            return MainResult::ErrorArguments;
        },
    };

    let mut futures = FuturesUnordered::new();

    for uri in uris.iter() {
        let fut = download_recursive_one(uri.clone(), target.clone(), recursive, progress.clone(), client.clone(), verbose, semaphore.clone(), transfer.clone());
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
