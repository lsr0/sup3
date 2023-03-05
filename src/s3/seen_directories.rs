pub struct SeenDirectories {
    seen: std::collections::hash_set::HashSet<String>,
    prefix_len: usize,
}

fn dirname(key: &str) -> &str {
    let trimmed = key.trim_end_matches(|c| c != '/');
    trimmed.strip_suffix('/').unwrap_or(trimmed)
}

fn up_directory(key: &str) -> &str {
    let without_slash = key.strip_suffix('/').unwrap_or(key);
    without_slash.rsplit_once('/').map(|(above, _below)| above).unwrap_or("")
}

impl SeenDirectories {
    pub fn new(prefix: &str) -> SeenDirectories {
        let prefix_len = if prefix.ends_with('/') { prefix.len() - 1 } else { prefix.len() };
        SeenDirectories { seen: Default::default(), prefix_len }
    }
    #[must_use]
    /// Returns all previously unseen directories
    /// from highest in the heirarchy to lowest
    pub fn add_key(&mut self, key: &str) -> Vec<String> {
        let mut missing = self.missing_directories(key);
        for d in &missing {
            self.seen.insert(d.clone());
        }
        missing.reverse();
        missing
    }
    fn missing_directories(&self, key: &str) -> Vec<String> {
        let mut missing = vec![];
        let mut search_key = dirname(key);
        while search_key.len() > self.prefix_len {
            if self.seen.contains(search_key) {
                return missing;
            }
            missing.push(search_key.to_owned());
            search_key = up_directory(search_key);
            if search_key.is_empty() {
                break;
            }
        }
        if search_key.len() > self.prefix_len {
            missing.push(search_key.to_owned());
        }
        missing
    }
}


#[test]
fn test_missing() {
    let mut seen = SeenDirectories::new("");
    let missing = seen.add_key("base/first/second/file.txt");
    assert_eq!(missing, vec!["base".to_string(), "base/first".to_string(), "base/first/second".to_string()]);
    let missing = seen.add_key("base/first/second2/pic.jpg");
    assert_eq!(missing, vec!["base/first/second2".to_string()]);
    let missing = seen.add_key("base2/first/second/pic.jpg");
    assert_eq!(missing, vec!["base2".to_string(), "base2/first".to_string(), "base2/first/second".to_string()]);
    let missing = seen.add_key("base3/first3/second3");
    assert_eq!(missing, vec!["base3".to_string(), "base3/first3".to_string()]);
}

#[test]
fn test_missing_prefixed() {
    let mut seen = SeenDirectories::new("base/");
    let missing = seen.add_key("base/first/second/file.txt");
    assert_eq!(missing, vec!["base/first".to_string(), "base/first/second".to_string()]);
    let missing = seen.add_key("base/first/second2/pic.jpg");
    assert_eq!(missing, vec!["base/first/second2".to_string()]);
}

