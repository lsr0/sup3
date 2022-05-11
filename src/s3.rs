use aws_types::region::Region;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{types::ByteStream, output::ListObjectsV2Output};
use futures::stream::Stream;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;

use crate::shared_options::SharedOptions;
use crate::cli;

mod uri;

pub use uri::{Uri, UriError};

pub struct Client {
    client: aws_sdk_s3::Client,
}

pub async fn init(region: Option<String>, endpoint: Option<http::uri::Uri>) -> Client {
    let provided_region = region.map(Region::new);
    let region_provider = RegionProviderChain::first_try(provided_region)
        .or_default_provider()
        .or_else("eu-west-1");
    let mut builder = aws_config::from_env().region(region_provider);
    if let Some(uri) = endpoint {
        let endpoint = aws_smithy_http::endpoint::Endpoint::mutable(uri);
        builder = builder.endpoint_resolver(endpoint);
    }
    let config = builder.load().await;
    let client = aws_sdk_s3::Client::new(&config);
    Client {
        client,
    }
}

#[derive(clap::Args, Debug)]
pub struct ListArguments {
    /// Display full S3 paths
    #[clap(long, short='F')]
    full_path: bool,
    /// Display long format (size, date, name)
    #[clap(long, short='l')]
    long: bool,
    /// List directories themselves, not their contents
    #[clap(long, short='d')]
    directory: bool,
    /// List substring matches
    #[clap(long, short='s')]
    substring: bool,
    /// Recurse into subdirectories
    #[clap(long, short='r')]
    recurse: bool,
}

#[derive (thiserror::Error, Debug)]
pub enum Error {
    #[error("S3: {0}")]
    S3(#[from] aws_sdk_s3::Error),
    #[error("S3 put error: {0}")]
    Put(#[from] aws_sdk_s3::error::PutObjectError),
    #[error("accessing local file: {0}")]
    File(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("no filename in either source or destination")]
    NoFilename,
    #[error("specified local filename not unicode")]
    LocalFilenameNotUnicode,
    #[error("local file: {0}")]
    LocalFile(#[from] std::io::Error),
    #[error("streaming: {0}")]
    Streaming(#[from] aws_smithy_http::byte_stream::Error),
    #[error("cancelled by Ctrl-C")]
    CtrlC,
}

impl Error {
    pub fn should_cancel_other_operations(&self) -> bool {
        matches!(self, Self::CtrlC)
    }
}

#[derive(Clone)]
struct ProgressCallback(cli::ProgressFn);

impl ProgressCallback {
    pub fn wrap(delegate: cli::ProgressFn) -> Box<dyn aws_smithy_http::callback::BodyCallback> {
        Box::new(ProgressCallback(delegate))
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
impl aws_smithy_http::callback::BodyCallback for ProgressCallback {
    fn update(&mut self, bytes: &[u8]) -> Result<(), BoxError> {
        (self.0)(cli::Update::StateProgress(bytes.len()));
        Ok(())
    }

    fn make_new(&self) -> Box<dyn aws_smithy_http::callback::BodyCallback> {
        Box::new(self.clone())
    }
}

pub enum Target {
    Directory(std::path::PathBuf),
    File(std::path::PathBuf),
}

impl Target {
    fn local_path(&self, from: &Uri) -> Result<std::path::PathBuf, Error> {
        match self {
            Self::File(path) => Ok(path.clone()),
            Self::Directory(path) => {
                let mut local_path = path.clone();
                local_path.push(from.filename().ok_or(Error::NoFilename)?);
                Ok(local_path)
            },
        }
    }
}

async fn cleanup_temporary_file(_file: tokio::fs::File, path: &std::path::Path) -> Result<(), std::io::Error> {
    #[cfg(target_family = "windows")]
    {
        let mut file_to_drop = _file;
        file_to_drop.flush().await?;
    }
    tokio::fs::remove_file(path).await?;
    Ok(())
}

impl Client {
    pub async fn put(&self, verbose: bool, path: &std::path::Path, s3_uri: &Uri, progress_fn: cli::ProgressFn) -> Result<String, Error> {
        progress_fn(cli::Update::State("opening"));
        let mut stream = ByteStream::from_path(path)
            .await
            .map_err(|e| Error::File(e.into()))?;
        let mut key = s3_uri.key.clone();
        let (_, size_hint) = stream.size_hint();
        stream.with_body_callback(ProgressCallback::wrap(progress_fn.clone()));
        if s3_uri.filename().is_none() {
            let local_filename = path.file_name()
                .ok_or(Error::NoFilename)?
                .to_str()
                .ok_or(Error::LocalFilenameNotUnicode)?;
            key.push_str(local_filename);
        }
        let path_printable = path.to_string_lossy();
        let destination = format!("s3://{}/{key}", s3_uri.bucket);
        if verbose {
            match size_hint {
                Some(size) => println!("🏁 uploading '{path_printable}' [{size} bytes] to {destination}"),
                None => println!("🏁 uploading '{path_printable}' to {destination}"),
            };
        }
        progress_fn(cli::Update::State("uploading"));
        if let Some(size) = size_hint {
            progress_fn(cli::Update::StateLength(size));
        }
        self.client.put_object()
            .bucket(s3_uri.bucket.clone())
            .key(key.clone())
            .body(stream)
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        progress_fn(cli::Update::Finished());
        Ok(destination)
    }
    pub async fn get(&self, verbose: bool, from: &Uri, to: &Target, progress_fn: cli::ProgressFn) -> Result<std::path::PathBuf, Error> {
        progress_fn(cli::Update::State("opening"));
        let local_path = to.local_path(from)?;
        let mut path_string_temporary = local_path.as_os_str().to_owned();
        path_string_temporary.push(".sup3.partial");
        let local_path_temporary = std::path::PathBuf::from(path_string_temporary);
        let path_printable = local_path.to_string_lossy();
        let local_file = tokio::fs::File::create(&local_path_temporary).await?;
        progress_fn(cli::Update::State("connecting"));
        let mut response = self.client.get_object()
            .bucket(from.bucket.clone())
            .key(from.key.clone())
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;

        progress_fn(cli::Update::State("downloading"));
        progress_fn(cli::Update::StateLength(response.content_length() as usize));
        if verbose {
            println!("🏁 downloading '{from}' [{size} bytes] to {path_printable}", size = response.content_length());
        }
        response.body.with_body_callback(ProgressCallback::wrap(progress_fn.clone()));
        let mut buffered_file = tokio::io::BufWriter::new(local_file);
        loop {
            let next_block = response.body.try_next();
            tokio::select!{
                _ = tokio::signal::ctrl_c() => {
                    cleanup_temporary_file(buffered_file.into_inner(), &local_path_temporary).await?;
                    return Err(Error::CtrlC);
                }
                result = next_block => match result {
                    Ok(Some(bytes)) => buffered_file.write_all(&bytes).await?,
                    Ok(None) => break,
                    Err(e) => return Err(e.into()),
                },
            }
        }
        tokio::fs::rename(local_path_temporary, &local_path).await?;
        progress_fn(cli::Update::Finished());
        Ok(local_path)
    }
    pub async fn remove(&self, opts: &SharedOptions, s3_uri: &Uri) -> Result<(), Error> {
        if opts.verbose {
            println!("🏁 removing s3://{}/{}... ", s3_uri.bucket, s3_uri.key);
        }
        self.client.delete_object()
            .bucket(s3_uri.bucket.clone())
            .key(s3_uri.key.clone())
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        Ok(())
    }

    async fn ls_inner(&self, bucket: &str, key: &str, delimiter: Option<char>) -> Result<ListObjectsV2Output, aws_sdk_s3::Error> {
        self.client.list_objects_v2()
            .bucket(bucket.to_owned())
            .prefix(key.to_owned())
            .set_delimiter(delimiter.map(|c| c.into()))
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )
    }
    pub async fn ls(&self, opts: &SharedOptions, args: &ListArguments, s3_uri: &Uri) -> Result<(), Error> {
        if opts.verbose {
            println!("🏁 listing s3://{}/{}... ", s3_uri.bucket, s3_uri.key);
        }
        let separator = if args.recurse { None } else { Some('/') };

        let response = self.ls_inner(&s3_uri.bucket, &s3_uri.key, separator)
            .await?;

        if !args.directory {
            if let Some(directories) = &response.common_prefixes {
                let file_count = &response.contents
                    .as_ref()
                    .map(|c| c.len())
                    .unwrap_or(0);
                let directory_name = s3_uri.key.clone() + "/";
                if *file_count == 0 && directories.len() == 1 && directories[0].prefix.as_ref() == Some(&directory_name) {
                    if opts.verbose {
                        eprintln!("+ result was a directory name, requesting directory listing s3://{}/{directory_name}...", s3_uri.bucket);
                    }
                    let directory_response = self.ls_inner(&s3_uri.bucket, &directory_name, separator)
                        .await?;
                    ls_consume_response(args, &directory_response, &directory_name, &s3_uri.bucket);
                    return Ok(());
                }
            }
        }

        ls_consume_response(args, &response, &s3_uri.key, &s3_uri.bucket);
        Ok(())
    }
}

const DATE_LEN: usize = "2022-01-01T00:00:00Z".len();

fn basename(path: &str) -> &str {
    path.trim_end_matches(|c| c != '/')
}

fn key_matches_requested(requested: &str, key: &str, args: &ListArguments) -> bool {
    if args.substring {
        return true
    }

    if requested == key {
        return true;
    }

    let requested_directory = requested.ends_with('/') || requested.is_empty();
    if requested_directory {
        let directory_path = basename(requested);
        return key.starts_with(directory_path);
    } else if args.recurse {
        if let Some(after_requested) = key.strip_prefix(requested) {
            return after_requested.starts_with('/');
        }
    }

    if Some(requested) == key.strip_suffix('/') {
        return true
    }

    false
}

fn is_requested_path_directory(response: &ListObjectsV2Output, requested_path: &str) -> bool {
    let files = response.contents.iter().flatten();
    for name in files.flat_map(|f| f.key.as_ref()) {
        if name.strip_prefix(requested_path).unwrap_or("").starts_with('/') {
            return true;
        }
    }
    false
}

fn ls_consume_response(args: &ListArguments, response: &ListObjectsV2Output, prefix: &str, bucket: &str) {
    let directory_prefix = if args.recurse && !prefix.ends_with('/') && is_requested_path_directory(response, prefix) {
        prefix.to_owned() + "/"
    } else {
        basename(prefix).to_owned()
    };

    let printable_filename = |key: &str| {
        if args.full_path {
            format!("s3://{bucket}/{}", if key == "/" { "" } else { key })
        } else {
            let filename = key.strip_prefix(&directory_prefix).unwrap_or(key);
            filename.to_owned()
        }
    };

    let max_file_size = response.contents.as_ref()
        .and_then(|c| c.iter().map(|file| file.size()).max())
        .unwrap_or(0);

    let size_width = cli::digit_count(max_file_size as u64);

    let mut seen_directories = std::collections::hash_set::HashSet::new();

    let print_directory = |name| {
        if !key_matches_requested(prefix, name, args) {
            return;
        }
        let name = printable_filename(name);
        if args.long {
            println!("{:size_width$} {:DATE_LEN$} {name}", 0, "");
        } else {
            println!("{name}");
        }
    };

    for dir in response.common_prefixes().unwrap_or_default() {
        if let Some(name) = &dir.prefix {
            print_directory(name);
        }
    }

    for file in response.contents().unwrap_or_default() {
        if let Some(name) = &file.key {
            if !key_matches_requested(prefix, name, args) {
                continue;
            }
            if args.recurse {
                let dir_path = basename(name);
                if (dir_path != prefix) && seen_directories.insert(dir_path) {
                    print_directory(dir_path);
                }
            }
            let name = printable_filename(name);
            if args.long {
                let date = file.last_modified()
                    .and_then(|d| d.fmt(aws_smithy_types::date_time::Format::DateTime).ok())
                    .unwrap_or_else(|| "".to_owned());
                println!("{:size_width$} {date:DATE_LEN$} {name}", file.size());
            } else {
                println!("{name}");
            }
        }
    }
}
