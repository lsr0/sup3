use crate::s3::uri;

use wax::Pattern;

#[derive(clap::ArgEnum, Debug, Clone, PartialEq)]
pub enum GlobOption {
    Auto,
    On,
    Off,
}

#[derive(clap::Args, Debug, Clone, PartialEq)]
pub struct Options {
    /// EXPERIMENTAL: Enable glob path specification (auto enables when glob characters found)
    #[clap(long, short='G', arg_enum, default_value="off")]
    glob: GlobOption,
}

impl Options {
    pub fn is_enabled(&self) -> bool {
        !matches!(self.glob, GlobOption::Off)
    }
}

#[derive(Debug)]
pub struct Glob<'a> {
    prefix: uri::Key,
    glob: wax::Glob<'a>,
    has_recursive_wildcard: bool,
}

impl<'a> Glob<'a> {
    pub fn new(key: &'a uri::Key, options: &Options) -> Option<Glob<'a>> {
        if options.glob == GlobOption::Off {
            return None;
        }

        if key.len() == 0 && options.glob != GlobOption::On {
            return None;
        }

        let glob = wax::Glob::new(key.as_str()).ok()?;
        let (prefix, glob) = glob.partition();

        if options.glob == GlobOption::Auto {
            if prefix.as_os_str() == key.as_str() {
                return None;
            }

            // wax::Glob stops prefixes at slash
            let key_without_slash = key.as_str().strip_suffix('/').unwrap_or(key.as_str());
            if prefix.as_os_str() == key_without_slash {
                return None;
            }
        }

        let mut prefix_string = prefix.as_os_str().to_str()?.to_string();
        if !prefix_string.is_empty() && !prefix_string.ends_with('/') {
            prefix_string.push('/');
        }
        let prefix = uri::Key::new(prefix_string);

        let has_recursive_wildcard = glob_has_resursive_wildcard(key.as_str());
        Some(Glob{prefix, glob, has_recursive_wildcard})
    }
    pub fn prefix(&self) -> &uri::Key {
        &self.prefix
    }
    pub fn matches(&self, key: &str) -> bool {
        let without_prefix = key.strip_prefix(self.prefix.as_str()).expect("key must contain prefix we fetched");
        let without_prefix_slash = without_prefix.strip_prefix('/').unwrap_or(without_prefix);
        let without_trailing_slash = without_prefix_slash.strip_suffix('/').unwrap_or(without_prefix_slash);
        self.glob.is_match(without_trailing_slash)
    }
    pub fn has_recursive_wildcard(&self) -> bool {
        self.has_recursive_wildcard
    }

}

pub fn as_key_and_glob<'a>(key: &'a uri::Key, options: &Options) -> Option<Glob<'a>> {
    Glob::new(key, options)
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
        _ => glob_str.chars().nth(index - 2) == Some('\\') || glob_str.chars().nth(index - 1) != Some('\\'),
    }
}

#[test]
fn test_has_recusive_wildcard() {
    assert_eq!(glob_has_resursive_wildcard("test/**"), true);
    assert_eq!(glob_has_resursive_wildcard("test/**/*.txt"), true);
    assert_eq!(glob_has_resursive_wildcard("test/**/*"), true);

    assert_eq!(glob_has_resursive_wildcard("**"), true);
    assert_eq!(glob_has_resursive_wildcard("\\**"), false);
    assert_eq!(glob_has_resursive_wildcard("\\\\**"), true);
    assert_eq!(glob_has_resursive_wildcard("test/**"), true);
    assert_eq!(glob_has_resursive_wildcard("test/\\**"), false);
    assert_eq!(glob_has_resursive_wildcard("test/\\\\**"), true);

    assert_eq!(glob_has_resursive_wildcard("test/*"), false);
}

