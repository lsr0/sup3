#[derive (Debug)]
pub struct Uri {
    pub bucket: String,
    pub key: String,
}

#[derive (thiserror::Error, Debug)]
pub enum UriError {
    #[error("error parsing url: {:?}", .source)]
    ParseError{
        #[from]
        source: url::ParseError
    },
    #[error("scheme was not s3://")]
    InvalidScheme,
    #[error("missing bucket")]
    MissingBucket,
}

impl std::str::FromStr for Uri {
    type Err = UriError;
    fn from_str(s: &str) -> Result<Uri, Self::Err> {
        let parsed = url::Url::parse(s)?;
        if parsed.scheme() != "s3" {
            return Err(UriError::InvalidScheme);
        }
        let bucket = match parsed.host() {
            None => return Err(UriError::MissingBucket),
            Some(b) => b,
        };
        Ok(Uri {
            bucket: bucket.to_string(),
            key: parsed.path().strip_prefix('/').expect("URL separator must be /").to_string(),
        })
    }
}

impl Uri {
    pub fn filename(&self) -> Option<&str> {
        match self.key.rsplit_once('/') {
            None if self.key.len() > 0 => Some(&self.key),
            None => None,
            Some((_, "")) => None,
            Some((_, filename)) => Some(filename),
        }
    }
}

impl std::fmt::Display for Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}
