use std::path::PathBuf;

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
mod seen_directories;

pub use uri::{Uri, UriError, Key};

#[derive(Clone)]
pub struct Client {
    client: aws_sdk_s3::Client,
    region: Option<Region>,
}

#[derive(clap::Args, Debug, Clone)]
pub struct OptionsUpload {
    /// Canned access control list. Known values:
    ///   private, public-read, public-read-write, aws-exec-read,
    ///   authenticated-read, bucket-owner-read,
    ///   bucket-owner-full-control
    #[clap(long, verbatim_doc_comment, help_heading="Access Control")]
    pub canned_acl: Option<aws_sdk_s3::model::ObjectCannedAcl>,
    /// Grant read access (comma separated, [id=|uri=|emailAddress=])
    #[clap(long, help_heading="Access Control")]
    pub grant_read: Option<String>,
    /// Grant full control (comma separated, [id=|uri=|emailAddress=])
    #[clap(long, help_heading="Access Control")]
    pub grant_full: Option<String>,
    /// Grant read ACL (comma separated, [id=|uri=|emailAddress=])
    #[clap(long, help_heading="Access Control")]
    pub grant_read_acp: Option<String>,
    /// Grant write ACL (comma separated, [id=|uri=|emailAddress=])
    #[clap(long, help_heading="Access Control")]
    pub grant_write_acp: Option<String>,
    /// Storage Class
    #[clap(long, possible_values=aws_sdk_s3::model::StorageClass::values())]
    pub class: Option<aws_sdk_s3::model::StorageClass>,
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
        region: config.region().cloned(),
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
    #[error("no such remote file: {0}")]
    NoSuchKey(Uri),
    #[error("io: {0}")]
    Io(std::io::Error),
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

#[derive (Clone)]
pub enum Target {
    Directory(PathBuf),
    File(PathBuf),
}

impl Target {
    pub fn new_create(uris: &[Uri], to: &PathBuf, recursive: bool) -> Result<Target, String> {
        match to.metadata() {
            Ok(meta) if meta.is_dir() => Ok(Target::Directory(to.clone())),
            Ok(_) if uris.len() > 1 => Err("multiple uris and destination is not a directory".to_owned()),
            Ok(_) => Ok(Target::File(to.clone())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if uris.len() > 1 || recursive {
                    std::fs::create_dir(to).map_err(|e| format!("failed to create directory: {e}"))?;
                    Ok(Target::Directory(to.clone()))
                } else {
                    Ok(Target::File(to.clone()))
                }
            },
            Err(err) => Err(err.to_string()),
        }
    }
    fn local_path(&self, from: &Uri) -> Result<PathBuf, Error> {
        match self {
            Self::File(path) => Ok(path.clone()),
            Self::Directory(path) => {
                let mut local_path = path.clone();
                local_path.push(from.filename().ok_or(Error::NoFilename)?);
                Ok(local_path)
            },
        }
    }
    pub fn path(&self) -> PathBuf {
        match self {
            Self::File(path) | Self::Directory(path) => path.clone()
        }
    }
    pub fn child(&self, child_directory: &str) -> Target {
        let mut path = self.path();
        path.push(child_directory);
        Self::Directory(path)
    }
}

async fn get_write_loop(local_file: &mut partial_file::PartialFile, mut body: aws_smithy_http::byte_stream::ByteStream) -> Result<(), Error> {
    loop {
        let next_block = body.try_next();
        match next_block.await {
            Ok(Some(bytes)) => local_file.writer().write_all(&bytes).await?,
            Ok(None) => break,
            Err(e) => return Err(e.into()),
        };
    }
    Ok(())
}

pub enum GetRecursiveResultStream<'a> {
    One(PathBuf),
    Many(RecursiveListStream<'a>),
}

pub enum RecursiveStreamItem {
    Directory(Key),
    File(Key),
}

pub struct RecursiveListStream<'a> {
    client: &'a Client,
    seen_directories: seen_directories::SeenDirectories,
    directory_uri: Uri,
    continuation_token: Option<String>,
    progress_fn: cli::ProgressFn,
}

impl<'a> RecursiveListStream<'a> {
    pub fn stream(&'a mut self) -> impl Stream<Item = Result<Vec<RecursiveStreamItem>, Error>> + 'a {
        async_stream::try_stream! {
            loop {
                let response = self.client.get_recursive_list_page(&self.directory_uri, &mut self.seen_directories, self.continuation_token.clone())
                    .await?;
                match response {
                    None => return (),
                    Some((page, continuation_token)) if continuation_token.is_some() => {
                        self.continuation_token = continuation_token;
                        yield page;
                    },
                    Some((page, _continuation_token)) => {
                        (self.progress_fn)(cli::Update::FinishedHide());
                        yield page;
                        return ();
                    },
                }
            }
        }
    }
}

impl Client {
    pub async fn put(&self, verbose: bool, options_upload: &OptionsUpload, path: &std::path::Path, s3_uri: &Uri, progress_fn: cli::ProgressFn) -> Result<String, Error> {
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
            .set_acl(options_upload.canned_acl.to_owned())
            .set_grant_read(options_upload.grant_read.to_owned())
            .set_grant_full_control(options_upload.grant_full.to_owned())
            .set_grant_read_acp(options_upload.grant_read_acp.to_owned())
            .set_grant_write_acp(options_upload.grant_write_acp.to_owned())
            .set_storage_class(options_upload.class.to_owned())
            .body(stream)
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        progress_fn(cli::Update::Finished());
        Ok(destination)
    }
    pub async fn get_recursive_stream(&self, verbose: bool, recursive: bool, from: Uri, to: Target, progress_fn: cli::ProgressFn) -> Result<GetRecursiveResultStream, Error> {
        progress_fn(cli::Update::State("listing"));
        match self.get(verbose, &from, &to, progress_fn.clone()).await {
            Err(Error::NoSuchKey(uri)) if recursive => {
                let recursive_stream = self.get_recursive_list_stream(&uri, progress_fn).await?;
                Ok(GetRecursiveResultStream::Many(recursive_stream))
            },
            Ok(path) => Ok(GetRecursiveResultStream::One(path)),
            Err(err) => Err(err),
        }
    }
    pub async fn get(&self, verbose: bool, from: &Uri, to: &Target, progress_fn: cli::ProgressFn) -> Result<PathBuf, Error> {
        // S3 errors on root key requests, wrap into no such key
        if from.key.is_empty() {
            return Err(Error::NoSuchKey(from.clone()));
        }
        progress_fn(cli::Update::State("connecting"));
        let mut response = self.client.get_object()
            .bucket(from.bucket.clone())
            .key(from.key.to_string())
            .send()
            .await
            .map_err(|e| error_from_get(from, e))?;

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
    pub async fn get_recursive_list_stream(&self, uri: &Uri, progress_fn: cli::ProgressFn) -> Result<RecursiveListStream, Error> {
        let key = uri.key.to_explicit_directory();
        let seen_directories = seen_directories::SeenDirectories::new(key.as_str());
        Ok(RecursiveListStream {
            client: self,
            seen_directories,
            directory_uri: Uri::new(uri.bucket.clone(), key),
            continuation_token: None,
            progress_fn,
        })
    }
    pub async fn get_recursive_list_page(&self, uri: &Uri, seen_directories: &mut seen_directories::SeenDirectories, continuation_token: Option<String>) -> Result<Option<(Vec<RecursiveStreamItem>, Option<String>)>, Error> {
        let files = self.ls_inner(&uri.bucket, &uri.key, None, continuation_token)
            .await?;
        let mut ret = Vec::new();
        for key in files.contents.unwrap_or_default()
            .into_iter()
            .flat_map(|f| f.key) {
            for dir in seen_directories.add_key(&key) {
                ret.push(RecursiveStreamItem::Directory(Key::new(dir)));
            }
            ret.push(RecursiveStreamItem::File(Key::new(key)));
        }
        let next_continuation_token = files.continuation_token;
        if ret.is_empty() {
            if next_continuation_token.is_some() {
                return Ok(None);
            } else {
                return Err(Error::NoSuchKey(uri.clone()));
            }
        }
        Ok(Some((ret, next_continuation_token)))
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

    async fn ls_inner(&self, bucket: &str, key: &Key, delimiter: Option<char>, continuation: Option<String>) -> Result<ListObjectsV2Output, aws_sdk_s3::Error> {
        self.client.list_objects_v2()
            .bucket(bucket.to_owned())
            .prefix(key.to_string())
            .set_delimiter(delimiter.map(|c| c.into()))
            .set_continuation_token(continuation)
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )
    }
    pub async fn ls(&self, opts: &SharedOptions, args: &ListArguments, s3_uri: &Uri) -> Result<(), Error> {
        if opts.verbose {
            println!("🏁 listing s3://{}/{}... ", s3_uri.bucket, s3_uri.key);
        }
        let separator = if args.recurse { None } else { Some('/') };

        let mut response = self.ls_inner(&s3_uri.bucket, &s3_uri.key, separator, None)
            .await?;
        let mut relative_root = s3_uri.key.clone();

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
                    let directory_response = self.ls_inner(&s3_uri.bucket, &directory_name, separator, None)
                        .await?;
                    response = directory_response;
                    relative_root = directory_name;
                }
            }
        }

        let directory_prefix = if args.recurse && !relative_root.is_explicitly_directory() && is_requested_path_directory(&response, &relative_root) {
            relative_root.to_explicit_directory()
        } else {
            relative_root.basename_key()
        };

        let mut seen_directories = seen_directories::SeenDirectories::new(&relative_root);
        ls_consume_response(args, &response, &directory_prefix, &s3_uri.bucket, &mut seen_directories);

        let mut continuation_token = response.next_continuation_token;
        let mut page = 2;
        while continuation_token.is_some() {
            if opts.verbose {
                println!("🏁 listing s3://{}/{} (page {page})... ", s3_uri.bucket, s3_uri.key);
            }
            let continuation_response = self.ls_inner(&s3_uri.bucket, &relative_root, separator, continuation_token.take())
                .await?;

            ls_consume_response(args, &continuation_response, &relative_root, &s3_uri.bucket, &mut seen_directories);
            continuation_token = continuation_response.next_continuation_token;
            page += 1;
        }
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
    pub async fn cat(&self, uri: &Uri) -> Result<(), Error> {
        let response = self.client.get_object()
            .bucket(uri.bucket.clone())
            .key(uri.key.to_string())
            .send()
            .await
            .map_err(|e| error_from_get(uri, e))?;

        let mut stdout = tokio::io::stdout();
        let mut body = response.body.into_async_read();
        tokio::io::copy(&mut body, &mut stdout)
            .await
            .map(|_| ())
            .map_err(Error::Io)
    }
    pub async fn make_bucket(&self, uri: &Uri) -> Result<(), Error> {
        let location_constraint = self.region.as_ref()
            .map(|r| r.as_ref().parse().expect("infallible"));
        let create_config = aws_sdk_s3::model::CreateBucketConfiguration::builder()
            .set_location_constraint(location_constraint)
            .build();

        self.client.create_bucket()
            .bucket(uri.bucket.clone())
            .create_bucket_configuration(create_config)
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        Ok(())
    }
}

fn error_from_get(uri: &Uri, sdk: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>) -> Error {
    if let aws_sdk_s3::types::SdkError::ServiceError{ref err, ..} = sdk {
        if err.is_no_such_key() {
            return Error::NoSuchKey(uri.clone());
        }
    }

    Error::S3(sdk.into())
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

fn ls_consume_response(args: &ListArguments, response: &ListObjectsV2Output, directory_prefix: &Key, bucket: &str, seen_directories: &mut seen_directories::SeenDirectories) {
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

    let print_directory = |name: &str| {
        if !key_matches_requested(directory_prefix, name, args) {
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
            if !key_matches_requested(directory_prefix, name, args) {
                continue;
            }
            if args.recurse {
                let dir_path = basename(name);
                if dir_path != directory_prefix.as_str() {
                    for unseen_directory in seen_directories.add_key(dir_path) {
                        print_directory(&unseen_directory);
                    }
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
