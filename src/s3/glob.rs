use crate::s3::uri;

use wax::Pattern;

#[derive(Debug)]
pub struct Glob<'a> {
    prefix: uri::Key,
    glob: wax::Glob<'a>,
}

impl<'a> Glob<'a> {
    pub fn new(key: &'a uri::Key) -> Option<Glob<'a>> {
        if key.len() == 0 {
            return None;
        }
        let glob = wax::Glob::new(key.as_str()).ok()?;
        let (prefix, glob) = glob.partition();

        if prefix.as_os_str() == key.as_str() {
            return None;
        }

        // wax::Glob stops prefixes at slash
        let key_without_slash = key.as_str().strip_suffix('/').unwrap_or(key.as_str());
        if prefix.as_os_str() == key_without_slash {
            return None;
        }

        let prefix = uri::Key::new(prefix.as_os_str().to_str()?.to_string());
        Some(Glob{prefix, glob})
    }
    pub fn prefix(&self) -> &uri::Key {
        &self.prefix
    }
    pub fn matches(&self, key: &str) -> bool {
        let key = dbg!(key);
        let without_prefix = key.strip_prefix(self.prefix.as_str()).expect("key must contain prefix we fetched");
        let without_prefix_slash = without_prefix.strip_prefix('/').unwrap_or(without_prefix);
        dbg!(self.glob.is_match(without_prefix_slash));
        let without_trailing_slash = without_prefix_slash.strip_suffix('/').unwrap_or(without_prefix_slash);
        let without_trailing_slash = dbg!(without_trailing_slash);
        self.glob.is_match(without_trailing_slash)
    }
}

pub fn as_key_and_glob<'a>(key: &'a uri::Key) -> Option<Glob<'a>> {
    Glob::new(key)
}
