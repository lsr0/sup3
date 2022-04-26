use aws_types::region::Region;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::ByteStream;
use futures::stream::Stream;

use crate::shared_options::SharedOptions;

mod uri;

pub use uri::{Uri, UriError};

pub struct Client {
    client: aws_sdk_s3::Client,
}

pub async fn init(region: Option<String>) -> Client {
    let provided_region = region.map(Region::new);
    let region_provider = RegionProviderChain::first_try(provided_region)
        .or_default_provider()
        .or_else("eu-west-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = aws_sdk_s3::Client::new(&config);
    Client {
        client,
    }
}

#[derive (thiserror::Error, Debug)]
pub enum Error {
    #[error("S3: {}", .source)]
    S3 {
        #[from]
        source: aws_sdk_s3::Error,
    },
    #[error("S3 put error: {}", .source)]
    Put {
        #[from]
        source: aws_sdk_s3::error::PutObjectError,
    },
    #[error("accessing local file: {}", .0)]
    File(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("no filename specified")]
    NoFilename,
    #[error("specified local filename not unicode")]
    LocalFilenameNotUnicode,
}

impl Client {
    pub async fn put(&self, opts: &SharedOptions, path: &std::path::Path, s3_uri: &Uri) -> Result<(), Error> {
        let stream = ByteStream::from_path(path)
            .await
            .map_err(|e| Error::File(e.into()))?;
        let mut key = s3_uri.key.clone();
        let (_, size_hint) = stream.size_hint();
        if s3_uri.filename().is_none() {
            let local_filename = path.file_name()
                .ok_or(Error::NoFilename)?
                .to_str()
                .ok_or(Error::LocalFilenameNotUnicode)?;
            key.push_str(local_filename);
        }
        key = key.trim_start_matches('/').to_string();
        let path_printable = path.to_string_lossy();
        if opts.verbose {
            match size_hint {
                Some(size) => println!("uploading '{path_printable}' [{size} bytes] to s3://{}/{key}", s3_uri.bucket),
                None => println!("uploading '{path_printable}' to s3://{}/{key}", s3_uri.bucket),
            };
        }
        self.client.put_object()
            .bucket(s3_uri.bucket.clone())
            .key(key)
            .body(stream)
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        Ok(())
    }
    pub async fn remove(&self, opts: &SharedOptions, s3_uri: &Uri) -> Result<(), Error> {
        if opts.verbose {
            println!("removing s3://{}/{}... ", s3_uri.bucket, s3_uri.key);
        }
        self.client.put_object()
            .bucket(s3_uri.bucket.clone())
            .key(s3_uri.key.clone())
            .send()
            .await
            .map_err(|e| -> aws_sdk_s3::Error { e.into() } )?;
        Ok(())
    }
}
