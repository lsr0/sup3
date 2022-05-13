use aws_types::region::Region;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{types::ByteStream, output::ListObjectsV2Output};
use futures::stream::Stream;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;

use crate::shared_options::SharedOptions;
use crate::cli;

mod uri;
mod partial_file;

pub use uri::{Uri, UriError, Key};

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

async fn get_write_loop(local_file: &mut partial_file::PartialFile, mut body: aws_smithy_http::byte_stream::ByteStream) -> Result<(), Error> {
    loop {
        let next_block = body.try_next();
        tokio::select!{
            _ = tokio::signal::ctrl_c() => {
                return Err(Error::CtrlC);
            }
            result = next_block => match result {
                Ok(Some(bytes)) => local_file.writer.write_all(&bytes).await?,
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            },
        }
    }
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
            key.push(local_filename);
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
            .key(key.to_string())
            .body(stream)
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        progress_fn(cli::Update::Finished());
        Ok(destination)
    }
    pub async fn get(&self, verbose: bool, from: &Uri, to: &Target, progress_fn: cli::ProgressFn) -> Result<std::path::PathBuf, Error> {
        progress_fn(cli::Update::State("connecting"));
        let mut response = self.client.get_object()
            .bucket(from.bucket.clone())
            .key(from.key.to_string())
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;

        progress_fn(cli::Update::State("opening"));
        let local_path = to.local_path(from)?;
        let mut local_file = partial_file::PartialFile::new(local_path).await?;

        progress_fn(cli::Update::State("downloading"));
        progress_fn(cli::Update::StateLength(response.content_length() as usize));
        if verbose {
            println!("🏁 downloading '{from}' [{size} bytes] to {path_printable}", size = response.content_length(), path_printable = local_file.path_printable());
        }
        response.body.with_body_callback(ProgressCallback::wrap(progress_fn.clone()));
        let local_path = match get_write_loop(&mut local_file, response.body).await {
            Ok(_) => local_file.finished().await?,
            Err(err) => {
                local_file.cancelled().await?;
                return Err(err);
            }
        };
        progress_fn(cli::Update::Finished());
        Ok(local_path)
    }
    pub async fn remove(&self, opts: &SharedOptions, s3_uri: &Uri) -> Result<(), Error> {
        if opts.verbose {
            println!("🏁 removing s3://{}/{}... ", s3_uri.bucket, s3_uri.key);
        }
        self.client.delete_object()
            .bucket(s3_uri.bucket.clone())
            .key(s3_uri.key.to_string())
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        Ok(())
    }

    async fn ls_inner(&self, bucket: &str, key: &Key, delimiter: Option<char>) -> Result<ListObjectsV2Output, aws_sdk_s3::Error> {
        self.client.list_objects_v2()
            .bucket(bucket.to_owned())
            .prefix(key.to_string())
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
                let directory_name = s3_uri.key.to_explicit_directory();
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
    pub async fn list_buckets(&self, opts: &SharedOptions) -> Result<(), Error> {
        if opts.verbose {
            println!("🏁 listing buckets... ");
        }
        let response = self.client.list_buckets()
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;

        for bucket in response.buckets.unwrap_or_default() {
            if let Some(name) = bucket.name {
                println!("{name}");
            }
        }

        Ok(())
    }
}

const DATE_LEN: usize = "2022-01-01T00:00:00Z".len();

fn basename(path: &str) -> &str {
    path.trim_end_matches(|c| c != '/')
}

fn key_matches_requested(requested: &Key, key: &str, args: &ListArguments) -> bool {
    if args.substring {
        return true
    }

    if requested.as_str() == key {
        return true;
    }

    let requested_directory = requested.is_explicitly_directory();
    if requested_directory {
        let directory_path = requested.basename();
        return key.starts_with(directory_path);
    } else if args.recurse {
        if let Some(after_requested) = key.strip_prefix(requested.as_str()) {
            return after_requested.starts_with('/');
        }
    }

    if Some(requested.as_str()) == key.strip_suffix('/') {
        return true
    }

    false
}

fn is_requested_path_directory(response: &ListObjectsV2Output, requested_path: &Key) -> bool {
    let files = response.contents.iter().flatten();
    for name in files.flat_map(|f| f.key.as_ref()) {
        if name.strip_prefix(requested_path.as_str()).unwrap_or("").starts_with('/') {
            return true;
        }
    }
    false
}

fn ls_consume_response(args: &ListArguments, response: &ListObjectsV2Output, prefix: &Key, bucket: &str) {
    let directory_prefix = if args.recurse && !prefix.is_explicitly_directory() && is_requested_path_directory(response, prefix) {
        prefix.to_explicit_directory()
    } else {
        prefix.basename_key()
    };

    let printable_filename = |key: &str| {
        if args.full_path {
            format!("s3://{bucket}/{}", if key == "/" { "" } else { key })
        } else {
            let filename = key.strip_prefix(directory_prefix.as_str()).unwrap_or(key);
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
                if (dir_path != prefix.as_str()) && seen_directories.insert(dir_path) {
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
