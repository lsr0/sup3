use clap::{Parser, Subcommand, Args};

use crate::shared_options::SharedOptions;
use crate::{s3, transfer, cli};

pub(crate) fn clap3_help_style() -> clap::builder::Styles {
    use clap::builder::styling::AnsiColor;
    clap::builder::Styles::styled()
        .header(AnsiColor::Yellow.on_default())
        .usage(AnsiColor::Green.on_default())
        .literal(AnsiColor::Green.on_default())
        .placeholder(AnsiColor::Green.on_default())
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None, styles = clap3_help_style())]
pub(crate) struct Arguments {
    #[clap(subcommand)]
    pub command: Commands,

    #[clap(long, short='R', global=true)]
    pub region: Option<String>,

    #[clap(long, short='e', global=true)]
    /// Use custom endpoint URL for other S3 implementations
    pub endpoint: Option<http::uri::Uri>,

    #[clap(long, global=true)]
    /// Override config profile name
    pub profile: Option<String>,

    #[clap(flatten)]
    pub shared: SharedOptions,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// Upload to S3
    #[clap(alias="up")]
    Upload(Upload),
    /// Download from S3
    #[clap(alias="down")]
    Download(Download),
    /// Remove from S3
    ///
    /// Note: will succeed if remote file doesn't exist
    Rm(Remove),
    /// List S3 path
    Ls(ListFiles),
    /// List S3 buckets
    #[clap(alias="lb")]
    ListBuckets(ListBuckets),
    /// Copy to/from S3, depending on arguments
    Cp(Copy),
    /// Print contents of S3 files
    Cat(Cat),
    /// Create S3 buckets
    #[clap(alias="mb")]
    MakeBuckets(MakeBuckets),
    #[cfg(feature = "gen-completion")]
    /// Generate CLI completion
    GenerateCompletion(GenerateCompletion),
}

#[derive(Args, Debug)]
pub(crate) struct Upload {
    #[clap(required = true, value_parser, value_hint=clap::ValueHint::AnyPath)]
    local_paths: Vec<std::path::PathBuf>,
    /// S3 URI in s3://bucket/path/components format
    #[clap(value_hint=clap::ValueHint::AnyPath)]
    to: s3::Uri,

    #[clap(flatten)]
    transfer: transfer::OptionsTransfer,

    #[clap(long, short = 'r')]
    recursive: bool,

    #[clap(flatten)]
    upload: s3::OptionsUpload,
}

#[derive(Args, Debug)]
pub(crate) struct Remove {
    /// S3 URI in s3://bucket/path/components format
    #[clap(required = true, value_hint=clap::ValueHint::Url)]
    remote_paths: Vec<s3::Uri>,
}

#[derive(Args, Debug)]
pub(crate) struct ListFiles {
    /// S3 URIs in s3://bucket/path/components format
    #[clap(required = true, value_hint=clap::ValueHint::Url)]
    remote_paths: Vec<s3::Uri>,
    #[clap(flatten)]
    command_args: s3::ListArguments,
}

#[derive(Args, Debug)]
pub(crate) struct Download {
    /// S3 URIs in s3://bucket/path/components format
    #[clap(required = true, num_args=1)]
    uris: Vec<s3::Uri>,
    #[clap(value_parser, value_hint=clap::ValueHint::AnyPath)]
    to: std::path::PathBuf,

    #[clap(flatten)]
    transfer: transfer::OptionsTransfer,

    #[clap(long, short = 'r')]
    recursive: bool,
}

#[derive(Args, Debug)]
pub(crate) struct ListBuckets {
}

#[derive(Args, Debug)]
pub(crate) struct Copy {
    /// Either <S3 URI..> <local path> or <local path..> <S3 URI>
    #[clap(required = true, value_parser=clap::value_parser!(std::ffi::OsString), value_hint=clap::ValueHint::AnyPath)]
    args: Vec<CopyArgument>,

    #[clap(flatten)]
    transfer: transfer::OptionsTransfer,

    #[clap(long, short = 'r')]
    recursive: bool,

    #[clap(flatten)]
    upload: s3::OptionsUpload,
}

#[derive(Args, Debug)]
pub(crate) struct Cat {
    /// S3 URIs in s3://bucket/path/components format
    #[clap(required = true, value_hint=clap::ValueHint::Url)]
    uris: Vec<s3::Uri>,
}

#[derive(Args, Debug)]
pub(crate) struct MakeBuckets {
    /// S3 URIs in s3://bucket format
    #[clap(required = true, value_hint=clap::ValueHint::Url)]
    buckets: Vec<s3::Uri>,
    /// Continue to next file on error
    #[clap(long, short='y')]
    continue_on_error: bool,

    #[clap(flatten)]
    s3_options: s3::OptionsMakeBucket,
}

#[cfg(feature = "gen-completion")]
#[derive(Args, Debug)]
pub(crate) struct GenerateCompletion {
    #[clap(required = true, value_enum)]
    shell: clap_complete::shells::Shell,
}

pub enum MainResult {
    Success,
    ErrorArguments,
    ErrorSomeOperationsFailed,
    Cancelled,
}

impl MainResult {
    pub fn from_error_count(count: u32) -> MainResult {
        match count {
            0 => MainResult::Success,
            _ => MainResult::ErrorSomeOperationsFailed,
        }
    }
}

impl std::process::Termination for MainResult {
    fn report(self) -> std::process::ExitCode {
        match self {
            Self::Success => std::process::ExitCode::SUCCESS,
            Self::ErrorArguments => std::process::ExitCode::from(1),
            Self::ErrorSomeOperationsFailed => std::process::ExitCode::from(2),
            Self::Cancelled => std::process::ExitCode::from(3),
        }
    }
}

impl Upload {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        transfer::upload(&self.local_paths, &self.to, client, opts, &self.transfer, &self.upload, self.recursive).await
    }
}

impl Download {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        transfer::download(&self.uris, &self.to, client, opts, &self.transfer, self.recursive).await
    }
}

impl Remove {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        for uri in &self.remote_paths {
            if let Err(e) = client.remove(opts, uri).await {
                eprintln!("❌: failed to remove {}: {e}", uri);
                return MainResult::ErrorSomeOperationsFailed;
            }
        }
        MainResult::Success
    }
}

impl ListFiles {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        if let Err(val_err) = self.command_args.validate() {
                use clap::CommandFactory;
                let _ = Arguments::command()
                    .error(val_err.0, val_err.1)
                    .print();
            return MainResult::ErrorArguments;
        };
        for uri in &self.remote_paths {
            if let Err(e) = client.ls(opts, &self.command_args, uri).await {
                eprintln!("❌: failed to list {uri}: {e}");
                return MainResult::ErrorSomeOperationsFailed;
            }
        }
        MainResult::Success
    }
}

impl ListBuckets {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        if let Err(e) = client.list_buckets(opts).await {
            eprintln!("❌: failed to list buckets: {e}");
            return MainResult::ErrorSomeOperationsFailed;
        }
        MainResult::Success
    }
}

/// Either an S3 URI or a local path
#[derive (Debug, Clone)]
pub enum CopyArgument {
    Uri(s3::Uri),
    LocalFile(std::path::PathBuf),
}

impl TryFrom<&std::ffi::OsStr> for CopyArgument {
    type Error = String;
    fn try_from(arg: &std::ffi::OsStr) -> Result<Self, String> {
        if let Some(unicode) = arg.to_str() {
            match unicode.parse() {
                Ok(uri) => return Ok(CopyArgument::Uri(uri)),
                Err(s3::UriError::ParseError{..}) => {},
                Err(other) => return Err(format!("{other}")),
            }
        }
        Ok(CopyArgument::LocalFile(std::path::PathBuf::from(arg)))
    }
}

impl Copy {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        let invalid_args = || {
            use clap::CommandFactory;
            let _ = Arguments::command()
                .error(clap::error::ErrorKind::ArgumentConflict, "cp requires either <S3 URI..> <local path> or <local path..> <S3 URI>")
                .print();
            MainResult::ErrorArguments
        };
        match &self.args[..] {
            [from @ .., CopyArgument::LocalFile(to)] => {
                let mut uris = vec![];
                for uri in from {
                    match uri {
                        CopyArgument::Uri(uri) => uris.push(uri.clone()),
                        CopyArgument::LocalFile(_) => return invalid_args(),
                    }
                }
                transfer::download(&uris, to, client, opts, &self.transfer, self.recursive).await
            },
            [from @ .., CopyArgument::Uri(to)] => {
                let mut paths = vec![];
                for path in from {
                    match path {
                        CopyArgument::LocalFile(path) => paths.push(path.clone()),
                        CopyArgument::Uri(_) => return invalid_args(),
                    }
                }
                transfer::upload(&paths, to, client, opts, &self.transfer, &self.upload, self.recursive).await
            },
            _ => invalid_args(),
        }
    }
}

impl Cat {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        for uri in &self.uris {
            if opts.verbose {
                eprintln!("🏁 cat '{uri}'");
            }
            if let Err(e) = client.cat(uri).await {
                cli::println_error(format_args!("failed to cat {uri}: {e}"));
                return MainResult::ErrorSomeOperationsFailed;
            }
        }
        MainResult::Success
    }
}

#[cfg(feature = "gen-completion")]
impl GenerateCompletion {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        use clap::CommandFactory;
        clap_complete::generate(self.shell, &mut Arguments::command(), clap::crate_name!(), &mut std::io::stdout());
        MainResult::Success
    }
}

impl MakeBuckets {
    pub(crate) async fn run(&self, client: &s3::Client, opts: &SharedOptions) -> MainResult {
        for uri in &self.buckets {
            if !uri.key.is_empty() {
                use clap::CommandFactory;
                let _ = Arguments::command()
                    .error(clap::error::ErrorKind::InvalidValue, "make_bucket requires pure bucket arguments without a key, e.g. 's3://bucketname/'")
                    .print();
                return MainResult::ErrorArguments;
            }
        }
        let mut error_count = 0;
        for uri in &self.buckets {
            if opts.verbose {
                eprintln!("🏁 mb '{uri}'");
            }
            if let Err(e) = client.make_bucket(uri, &self.s3_options).await {
                cli::println_error(format_args!("failed to create bucket {uri}: {e}"));
                if !self.continue_on_error {
                    return MainResult::ErrorSomeOperationsFailed;
                } else {
                    error_count += 1;
                }
            }
        }
        MainResult::from_error_count(error_count)
    }
}

