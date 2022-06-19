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

    pub struct Bar {
        bar: indicatif::ProgressBar,
        name: String,
    }
    pub struct Output {
        enabled: bool,
        multi: indicatif::MultiProgress,
        bars: std::sync::Mutex<Vec<Bar>>,
    }
    impl Output {
        pub fn new(args: &ArgProgress) -> Output {
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
            }
        }
        pub fn progress_enabled(&self) -> bool {
            self.enabled
        }
        pub fn add(&self, initial_state: impl Into<String>, name: String) -> ProgressFn {
            if !self.enabled {
                return Arc::new(|_update: Update| {});
            }

            let bar = indicatif::ProgressBar::new(1)
                .with_message(initial_state.into());
            bar.set_style(indicatif::ProgressStyle::with_template("{prefix:20.dim} {msg:>11.bold} {bytes:>10.cyan}/{total_bytes:>10.italic.250} {binary_bytes_per_sec:>11} {elapsed:>4} [{wide_bar:.red/blue}]")
                .unwrap()
                .progress_chars("#>-"));

            let bar = self.multi.add(bar);

            self.add_bar(Bar {
                bar: bar.clone(),
                name,
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
        fn add_bar(&self, added: Bar) {
            let mut bars = self.bars.lock().unwrap();
            bars.push(added);
            if bars.len() == 1 {
                let added = bars.get(0).expect("just added");
                added.bar.set_prefix(added.name.clone());
            } else {
                let name_len = bars.iter().map(|bar| bar.name.len()).max().unwrap_or(0);
                let mut index = 0;
                for bar in bars.iter() {
                    if bar.bar.is_hidden() {
                        continue;
                    }
                    let digits = digit_count(bars.len() as u64);
                    let grey = console::Style::new().color256(252);
                    let lb = grey.apply_to("(");
                    let rb = grey.apply_to(")");
                    bar.bar.set_prefix(format!("{lb}{:digits$}/{}{rb} {name:name_len$}", index + 1, bars.len(), name = bar.name));
                    index += 1;
                }
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
        pub fn println_done(&self, args: std::fmt::Arguments) {
            self.println(&PREFIX_DONE, args);
        }
        pub fn mark_cancelled(&self) {
            if !self.enabled {
                return;
            }
            let bars = self.bars.lock().unwrap();
            for bar in bars.iter() {
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

