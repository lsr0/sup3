use clap::Args;

#[derive(Args, Debug)]
pub struct SharedOptions {
    #[clap(long, short='v', global = true)]
    pub verbose: bool,
}
