use crate::s3::uri;

use wax::Pattern;

pub struct Glob<'a> {
    prefix: String,
    glob: wax::Glob<'a>,
}

impl<'a> Glob<'a> {
    pub fn matches(&self, key: &str) -> bool {
        self.glob.is_match(key)
    }
}

pub fn as_key_and_glob<'a>(key: &'a uri::Key) -> Option<(uri::Key, Glob<'a>)> {
    let glob = wax::Glob::new(key.as_str()).ok()?;
    let (prefix, glob) = glob.partition();
    if prefix.as_os_str() == key.as_str() {
        return None;
    }
    let prefix_key = uri::Key::new(prefix.as_os_str().to_str()?.to_string());
    Some((prefix_key, Glob{glob}))
}
