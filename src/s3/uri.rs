#[derive (Clone, Debug, PartialEq)]
pub struct Key(String);

#[derive (Clone, Debug)]
pub struct Uri {
    pub bucket: String,
    pub key: Key,
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
    #[error("invalid url component provided: {0}")]
    InvalidUrlComponents(&'static str),
}

impl std::str::FromStr for Uri {
    type Err = UriError;
    fn from_str(s: &str) -> Result<Uri, Self::Err> {
        let parsed = url::Url::parse(s)?;
        if parsed.scheme() != "s3" {
            return Err(UriError::InvalidScheme);
        }

        parsed.query().is_none().then(|| ()).ok_or(UriError::InvalidUrlComponents("query string"))?;
        parsed.username().is_empty().then(|| ()).ok_or(UriError::InvalidUrlComponents("username"))?;
        parsed.password().is_none().then(|| ()).ok_or(UriError::InvalidUrlComponents("password"))?;
        parsed.fragment().is_none().then(|| ()).ok_or(UriError::InvalidUrlComponents("fragment"))?;

        let bucket = match parsed.host() {
            None => return Err(UriError::MissingBucket),
            Some(b) => b,
        };
        let path = parsed.path();
        let key = if path.is_empty() { "".to_owned() } else { path.strip_prefix('/').expect("separator must be /").to_owned() };
        Ok(Uri {
            bucket: bucket.to_string(),
            key: Key(key),
        })
    }
}

pub fn filename(key: &str) -> Option<&str> {
    match key.rsplit_once('/') {
        None if !key.is_empty() => Some(key),
        None => None,
        Some((_, "")) => None,
        Some((_, filename)) => Some(filename),
    }
}

impl Key {
    pub fn new(key: String) -> Key {
        Key(key)
    }
    pub fn filename(&self) -> Option<&str> {
        filename(&self.0)
    }
    pub fn is_explicitly_directory(&self) -> bool {
        self.0.ends_with('/') || self.0.is_empty()
    }
    pub fn to_explicit_directory(&self) -> Key {
        let mut key = self.0.clone();
        if !self.is_explicitly_directory() {
            key.push('/');
        }
        Key(key)
    }
    pub fn push(&mut self, component: &str) {
        self.0.push_str(component);
    }
    pub fn basename(&self) -> &str {
        self.0.trim_end_matches(|c| c != '/')
    }
    pub fn basename_key(&self) -> Key {
        Key(self.basename().to_owned())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn as_directory_component(&self) -> &str {
        let without_slash = self.0.strip_suffix('/').unwrap_or(&self.0);
        without_slash.rsplit_once('/').map(|(_, component)| component).unwrap_or("")
    }
}

impl core::ops::Deref for Key {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Uri {
    pub fn new(bucket: String, key: Key) -> Uri {
        Uri { bucket, key }
    }
    pub fn filename(&self) -> Option<&str> {
        self.key.filename()
    }
}

impl std::fmt::Display for Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}
