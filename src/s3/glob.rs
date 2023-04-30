use crate::s3::uri;

use wax::Pattern;

#[derive(Debug)]
pub struct Glob<'a> {
    prefix: uri::Key,
    glob: wax::Glob<'a>,
    has_recursive_wildcard: bool,
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
        let has_recursive_wildcard = Self::glob_has_resursive_wildcard(key.as_str());
        Some(Glob{prefix, glob, has_recursive_wildcard})
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
    pub fn has_recursive_wildcard(&self) -> bool {
        self.has_recursive_wildcard
    }

    fn glob_has_resursive_wildcard(glob_str: &str) -> bool {
        let Some(index) = glob_str.find("**") else {
            return false
        };
        // Check not escaped
        match index {
            0 => true,
            // '\**' => not a recursive wildcard
            1 => glob_str.chars().nth(0) != Some('\\'),
            // '\**' => not a recursive wildcard
            // '\\**' => is a recursive wildcard (prefixed by a literal backslash)
            _ => glob_str.chars().nth(index - 2) != Some('\\') || glob_str.chars().nth(index - 1) == Some('\\'),
        }
    }
}

pub fn as_key_and_glob<'a>(key: &'a uri::Key) -> Option<Glob<'a>> {
    Glob::new(key)
}
