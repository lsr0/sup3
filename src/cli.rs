#[derive(clap::ArgEnum, Debug, Clone)]
pub enum ProgressOption {
    On,
    Off,
    /// Enable if stdout/stderr are a termimal
    Auto,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ArgProgress {
    /// Display transfer progress
    #[cfg(feature = "progress")]
    #[clap(long, short='p', arg_enum, default_value="auto")]
    progress: ProgressOption,
}

#[derive(Debug)]
pub enum Update {
    State(&'static str),
    StateLength(usize),
    StateProgress(usize),
    Finished(),
    FinishedHide(),
    Error(String),
}

pub fn digit_count(num: u64) -> usize {
    if num == 0 {
        return 1;
    }
    ((num as f32).log10() + 1f32) as u8 as usize
}


fn stderr_println(prefix: &impl std::fmt::Display, args: std::fmt::Arguments) {
    eprintln!("{prefix}{args}");
}

/// Use only if no Output extant
pub fn println_error(args: std::fmt::Arguments) {
    stderr_println(&PREFIX_ERROR, args)
}

#[cfg(feature = "progress")]
mod progress_enabled {
    use std::sync::Arc;
    use super::*;
    pub type ProgressFn = Arc<dyn Fn(Update) + Send + Sync + 'static>;

    pub const PREFIX_ERROR: console::Emoji = console::Emoji("❌ ", "");
    pub const PREFIX_DONE: console::Emoji = console::Emoji("✅ ", "");
    #[allow(unused)]
    pub const PREFIX_DEBUG: console::Emoji = console::Emoji("🐛 ", "");

    pub struct Bar {
        bar: indicatif::ProgressBar,
        name: String,
    }
    #[derive (Default)]
    pub struct Bars {
        bars: Vec<Bar>,
        incoming_task_count: usize,
    }
    pub struct Output {
        enabled: bool,
        verbose: bool,
        multi: indicatif::MultiProgress,
        bars: std::sync::Mutex<Bars>,
        hidden_path_prefix: String,
    }
    impl Output {
        pub fn new(args: &ArgProgress, verbose: bool, hidden_path_prefix: Option<String>) -> Output {
            let draw_target = indicatif::ProgressDrawTarget::stderr_with_hz(6);
            let enabled = match args.progress {
                ProgressOption::On => true,
                ProgressOption::Off => false,
                ProgressOption::Auto => console::user_attended() && console::user_attended_stderr(),
            };
            Output {
                enabled: enabled && !draw_target.is_hidden(),
                multi: indicatif::MultiProgress::with_draw_target(draw_target),
                bars: Default::default(),
                verbose,
                hidden_path_prefix: hidden_path_prefix.unwrap_or_default(),
            }
        }
        pub fn progress_enabled(&self) -> bool {
            self.enabled
        }
        pub fn add(&self, initial_state: impl Into<String>, name: String) -> ProgressFn {
            if !self.enabled {
                return Arc::new(move |_: Update| {});
            }

            let bar = indicatif::ProgressBar::new(1)
                .with_message(initial_state.into());
            bar.set_style(indicatif::ProgressStyle::with_template("{prefix:20.dim} {msg:>11.bold} {bytes:>10.cyan}/{total_bytes:>10.italic.250} {binary_bytes_per_sec:>11} {elapsed:>4} [{wide_bar:.cyan/blue.bold}]")
                .unwrap()
                .progress_chars("#>-"));

            let bar = self.multi.add(bar);

            self.add_bar(Bar {
                bar: bar.clone(),
                name: name.strip_prefix(&self.hidden_path_prefix).map(Into::into).unwrap_or(name),
            });

            Arc::new(move |update: Update| {
                match update {
                    Update::State(state_name) => bar.set_message(state_name),
                    Update::StateLength(total) => bar.set_length(total as u64),
                    Update::StateProgress(inc_completed) => bar.inc(inc_completed as u64),
                    Update::Finished() => bar.finish_with_message("done"),
                    Update::FinishedHide() => { bar.finish_and_clear(); bar.set_draw_target(indicatif::ProgressDrawTarget::hidden()); },
                    Update::Error(err) => bar.abandon_with_message(format!("{PREFIX_ERROR}failed: {err}")),
                }
            })
        }
        pub fn add_incoming_tasks(&self, count: usize) {
            if !self.enabled {
                return
            }
            let mut bars = self.bars.lock().unwrap();
            bars.incoming_task_count += count;
            self.update_bars(bars);
        }
        fn add_bar(&self, added: Bar) {
            let mut bars = self.bars.lock().unwrap();
            bars.bars.push(added);
            if bars.incoming_task_count > 0 {
                bars.incoming_task_count -= 1;
            }
            let task_count = bars.bars.len() + bars.incoming_task_count;
            if task_count == 1 {
                let added = bars.bars.get(0).expect("just added");
                added.bar.set_prefix(added.name.clone());
            } else {
                self.update_bars(bars);
            }
        }
        fn update_bars(&self, bars: std::sync::MutexGuard<Bars>) {
            let count_visible = bars.bars.iter().filter(|bar| !bar.bar.is_hidden()).count();
            let task_count = count_visible + bars.incoming_task_count;
            let name_len = bars.bars.iter().map(|bar| bar.name.len()).max().unwrap_or(0);
            let mut index = 0;
            for bar in bars.bars.iter() {
                if bar.bar.is_hidden() {
                    continue;
                }
                let digits = digit_count(task_count as u64);
                let grey = console::Style::new().color256(252);
                let lb = grey.apply_to("(");
                let rb = grey.apply_to(")");
                bar.bar.set_prefix(format!("{lb}{:digits$}/{}{rb} {name:name_len$}", index + 1, task_count, name = bar.name));
                index += 1;
            }
        }
        pub fn println(&self, prefix: &impl std::fmt::Display, args: std::fmt::Arguments) {
            if !self.enabled {
                stderr_println(prefix, args);
            } else {
                self.multi.println(format!("{prefix}{args}")).unwrap();
            }
        }
        pub fn println_error(&self, args: std::fmt::Arguments) {
            self.println(&PREFIX_ERROR, args);
        }
        pub fn println_error_noprogress(&self, args: std::fmt::Arguments) {
            if self.enabled {
                return;
            }
            self.println(&PREFIX_ERROR, args);
        }
        pub fn println_done_verbose(&self, args: std::fmt::Arguments) {
            if !self.verbose || self.enabled {
                return;
            }
            self.println(&PREFIX_DONE, args);
        }
        pub fn mark_cancelled(&self) {
            if !self.enabled {
                return;
            }
            let bars = self.bars.lock().unwrap();
            for bar in bars.bars.iter() {
                if bar.bar.is_hidden() || bar.bar.is_finished() {
                    continue;
                }
                bar.bar.abandon_with_message(format!("{PREFIX_ERROR}cancelled"));
            }
        }
    }
}

#[cfg(feature = "progress")]
pub use progress_enabled::*;

#[cfg(not(feature = "progress"))]
mod progress_disabled {
    use super::*;
    pub fn empty_progress_fn(_update: Update) { }
    pub type ProgressFn = fn(Update);

    pub const PREFIX_ERROR: &'static str = "❌ ";
    pub const PREFIX_DONE: &'static str = "✅ ";
    #[allow(unused)]
    pub const PREFIX_DEBUG: &'static str = "🐛 ";

    #[derive(Default)]
    pub struct Output {
    }
    impl Output {
        pub fn new(_args: &ArgProgress, _task_count: usize) -> Output {
            Output { }
        }
        pub fn progress_enabled(&self) -> bool {
            false
        }
        pub fn add(&self, _index: usize, _initial_state: impl Into<String>, _name: String) -> ProgressFn {
            empty_progress_fn
        }
        pub fn add_incoming_tasks(&self, _count: usize) {
        }
        pub fn println_error(&self, args: std::fmt::Arguments) {
            stderr_println(&PREFIX_ERROR, args);
        }
        pub fn println_done(&self, args: std::fmt::Arguments) {
            stderr_println(&PREFIX_DONE, args);
        }
        pub fn mark_cancelled(&self) {
        }
    }
}

#[cfg(not(feature = "progress"))]
pub use progress_disabled::*;

/// Common path component prefix
/// e.g. ["/r/a1/b.txt", "/r/a2/b.txt"] -> "/r/"
pub fn longest_file_display_prefix(mut strings: impl Iterator<Item = String>) -> String {
    let mut longest: String = match strings.next() {
        None => return "".into(),
        Some(s) => s.to_string(),
    };
    for item in strings {
        let count_same = item.chars().zip(longest.chars()).take_while(|(s, l)| s == l).count();
        longest.truncate(count_same);
    }
    // Trim back to last common path component
    match longest.rfind('/') {
        Some(ind) => longest.truncate(ind + 1),
        _ => {},
    }
    longest
}

#[test]
fn test_longest_file_display_prefix()
{
    #[track_caller]
    fn test_static(filenames: &[&'static str], expected_prefix: &'static str) {
        let owned_iter = filenames.iter().map(|s| s.to_string());
        assert_eq!(longest_file_display_prefix(owned_iter), expected_prefix);
    }
    test_static(&[
        "s3://bucket/dir/second/1.ext",
        "s3://bucket/dir/second/2.ext",
    ],
        "s3://bucket/dir/second/"
    );
    test_static(&[
        "s3://bucket/dir/second/1.ext",
        "s3://otherbucket/dir/second/2.ext",
    ],
        "s3://"
    );
    test_static(&[
        "s3://bucket/dir/second/1.ext",
        "s3://bucket/dir/different/2.ext",
    ],
        "s3://bucket/dir/"
    );
    test_static(&[
        "s3://bucket/dir/second/1.ext",
        "s3://bucket2/dir/second/2.ext",
    ],
        "s3://"
    );
    test_static(&[
        "s3://bucket/prog-v0.5.0/1.ext",
        "s3://bucket/prog-v0.6.0/1.ext",
    ],
        "s3://bucket/"
    );
}

