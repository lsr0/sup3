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

#[cfg(feature = "progress")]
mod progress_enabled {
    use std::sync::Arc;
    use super::*;
    pub type ProgressFn = Arc<dyn Fn(Update) + Send + Sync + 'static>;

    const PREFIX_ERROR: console::Emoji = console::Emoji("❌ ", "");
    const PREFIX_DONE: console::Emoji = console::Emoji("✅ ", "");

    #[derive(Default)]
    pub struct Output {
        enabled: bool,
        task_count: usize,
        multi: indicatif::MultiProgress,
    }
    impl Output {
        pub fn new(args: &ArgProgress, task_count: usize) -> Output {
            let draw_target = indicatif::ProgressDrawTarget::stderr_with_hz(6);
            let enabled = match args.progress {
                ProgressOption::On => true,
                ProgressOption::Off => false,
                ProgressOption::Auto => console::user_attended() && console::user_attended_stderr(),
            };
            Output {
                enabled: enabled && !draw_target.is_hidden(),
                task_count,
                multi: indicatif::MultiProgress::with_draw_target(draw_target),
                ..Default::default()
            }
        }
        pub fn progress_enabled(&self) -> bool {
            self.enabled
        }
        pub fn add(&self, index: usize, initial_state: impl Into<String>, name: String) -> ProgressFn {
            if !self.enabled {
                return Arc::new(|_update: Update| {});
            }

            let bar = indicatif::ProgressBar::new(1)
                .with_message(initial_state.into());
            bar.set_style(indicatif::ProgressStyle::with_template("{prefix:20.dim} {msg:>10.bold} {bytes:>10.cyan}/{total_bytes:>10.italic.250} {binary_bytes_per_sec:>11} {elapsed:>4} [{wide_bar:.red/blue}]")
                .unwrap()
                .progress_chars("#>-"));

            let bar = self.multi.add(bar);

            if self.task_count > 1 {
                let digits = digit_count(self.task_count as u64);
                let grey = console::Style::new().color256(252);
                let lb = grey.apply_to("(");
                let rb = grey.apply_to(")");
                bar.set_prefix(format!("{lb}{:digits$}/{}{rb} {name}", index + 1, self.task_count));
            } else {
                bar.set_prefix(format!("{name}"));
            }

            Arc::new(move |update: Update| {
                match update {
                    Update::State(state_name) => bar.set_message(format!("{state_name}")),
                    Update::StateLength(total) => bar.set_length(total as u64),
                    Update::StateProgress(inc_completed) => bar.inc(inc_completed as u64),
                    Update::Finished() => bar.finish_with_message("done"),
                    Update::Error(err) => bar.abandon_with_message(format!("{PREFIX_ERROR}failed: {err}")),
                }
            })
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
    }
}

#[cfg(feature = "progress")]
pub use progress_enabled::*;

#[cfg(not(feature = "progress"))]
mod progress_disabled {
    use super::*;
    pub fn empty_progress_fn(_update: Update) { }
    pub type ProgressFn = fn(Update);

    const PREFIX_ERROR: &'static str = "❌ ";
    const PREFIX_DONE: &'static str = "✅ ";

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
    }
}

#[cfg(not(feature = "progress"))]
pub use progress_disabled::*;

