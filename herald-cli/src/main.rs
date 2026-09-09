mod admin;
mod client;
mod config;
mod error;
mod handler;
mod poller;
mod streamer;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::client::HeraldClient;
use crate::config::{Config, ConnectionMode};

#[derive(Parser)]
#[command(name = "herald", about = "Local daemon for Herald webhook relay")]
struct Cli {
    /// Path to config file
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the daemon (poll or stream based on config)
    Run,

    /// Validate the config file
    Check,

    /// Show the resolved config
    Show,

    /// Create an account and print its API key
    Register {
        /// Customer id: ^[a-z0-9][a-z0-9-]{2,31}$, and not a reserved word
        customer_id: String,
        /// Registration secret, if the server gates /register.
        /// Falls back to HERALD_REGISTER_SECRET.
        #[arg(long)]
        secret: Option<String>,
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        json: bool,
    },

    /// Queue depth for an endpoint, without leasing anything
    Depth {
        endpoint: String,
        #[command(flatten)]
        auth: AuthArgs,
        #[arg(long)]
        json: bool,
    },

    /// Lease messages from an endpoint.
    ///
    /// This mutates the queue: leased messages move to in-flight, their
    /// deliver_count increments, and they redeliver unless ACKed before the
    /// visibility timeout. Use `depth` to look without leasing.
    Poll {
        endpoint: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long, default_value_t = 300)]
        visibility_timeout: u64,
        #[command(flatten)]
        auth: AuthArgs,
        #[arg(long)]
        json: bool,
    },

    /// Acknowledge one or more processed messages
    Ack {
        endpoint: String,
        #[arg(required = true)]
        message_ids: Vec<String>,
        #[command(flatten)]
        auth: AuthArgs,
        #[arg(long)]
        json: bool,
    },

    /// Requeue a message, or send it to the DLQ with --dlq
    Nack {
        endpoint: String,
        message_id: String,
        /// Send to the dead letter queue instead of requeueing. Note that this
        /// server exposes no route to read the DLQ back.
        #[arg(long)]
        dlq: bool,
        #[command(flatten)]
        auth: AuthArgs,
        #[arg(long)]
        json: bool,
    },

    /// Extend the visibility timeout on an in-flight message
    Heartbeat {
        endpoint: String,
        message_id: String,
        #[arg(long)]
        extend: Option<u64>,
        #[command(flatten)]
        auth: AuthArgs,
        #[arg(long)]
        json: bool,
    },

    /// Show account and billing state, or set the tier
    Account {
        /// free | standard | pro | enterprise. Omit to read current state.
        #[arg(long)]
        tier: Option<String>,
        #[command(flatten)]
        auth: AuthArgs,
        #[arg(long)]
        json: bool,
    },
}

/// Server and credential overrides shared by every authenticated admin command.
#[derive(clap::Args)]
struct AuthArgs {
    #[arg(long, global = true)]
    server: Option<String>,
    /// Falls back to HERALD_API_KEY, then the config file.
    #[arg(long, global = true)]
    api_key: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(Config::default_path);

    match cli.command {
        Commands::Check => {
            match Config::load(&config_path) {
                Ok(config) => {
                    println!("Config valid: {}", config_path.display());
                    println!("  Server: {}", config.server);
                    println!("  Connection: {:?}", config.connection);
                    println!("  Handlers: {}", config.handlers.len());
                    for (name, handler) in &config.handlers {
                        println!("    {name}: {} {}", handler.command, handler.args.join(" "));
                    }
                }
                Err(e) => {
                    eprintln!("Config error: {e}");
                    std::process::exit(1);
                }
            }
        }

        Commands::Show => {
            match Config::load(&config_path) {
                Ok(config) => {
                    println!("{}", serde_yaml::to_string(&serde_json::to_value(&config).unwrap_or_default()).unwrap_or_default());
                }
                Err(e) => {
                    eprintln!("Config error: {e}");
                    std::process::exit(1);
                }
            }
        }

        Commands::Register { customer_id, secret, server, json } => {
            let server = match admin::resolve_server(server, &config_path) {
                Ok(s) => s,
                Err(e) => { eprintln!("{e}"); std::process::exit(1); }
            };
            if let Err(e) = admin::register(server, customer_id, secret, json).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Depth { endpoint, auth, json } => {
            run_admin(auth, &config_path, json, |t, json| async move {
                admin::depth(t, endpoint, json).await
            }).await;
        }

        Commands::Poll { endpoint, limit, visibility_timeout, auth, json } => {
            run_admin(auth, &config_path, json, |t, json| async move {
                admin::poll(t, endpoint, limit, visibility_timeout, json).await
            }).await;
        }

        Commands::Ack { endpoint, message_ids, auth, json } => {
            run_admin(auth, &config_path, json, |t, json| async move {
                admin::ack(t, endpoint, message_ids, json).await
            }).await;
        }

        Commands::Nack { endpoint, message_id, dlq, auth, json } => {
            run_admin(auth, &config_path, json, |t, json| async move {
                admin::nack(t, endpoint, message_id, dlq, json).await
            }).await;
        }

        Commands::Heartbeat { endpoint, message_id, extend, auth, json } => {
            run_admin(auth, &config_path, json, |t, json| async move {
                admin::heartbeat(t, endpoint, message_id, extend, json).await
            }).await;
        }

        Commands::Account { tier, auth, json } => {
            run_admin(auth, &config_path, json, |t, json| async move {
                admin::account(t, tier, json).await
            }).await;
        }

        Commands::Run => {
            let config = match Config::load(&config_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to load config from {}: {e}", config_path.display());
                    eprintln!("Create a config file at {} or use --config <path>", Config::default_path().display());
                    std::process::exit(1);
                }
            };

            if config.handlers.is_empty() {
                eprintln!("No handlers configured. Add handlers to your config file.");
                std::process::exit(1);
            }

            tracing::info!(
                server = %config.server,
                connection = ?config.connection,
                handlers = config.handlers.len(),
                "starting herald-cli"
            );

            match config.connection {
                ConnectionMode::Poll => {
                    let client = HeraldClient::new(&config.server, &config.api_key);
                    if let Err(e) = poller::run(&config, &client).await {
                        tracing::error!(error = %e, "poller error");
                        std::process::exit(1);
                    }
                }
                ConnectionMode::Websocket => {
                    if let Err(e) = streamer::run(&config).await {
                        tracing::error!(error = %e, "streamer error");
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

/// Resolve the target, run an admin command, and exit non-zero on failure.
async fn run_admin<F, Fut>(
    auth: AuthArgs,
    config_path: &PathBuf,
    json: bool,
    f: F,
) where
    F: FnOnce(admin::Target, bool) -> Fut,
    Fut: std::future::Future<Output = Result<(), error::CliError>>,
{
    let target = match admin::resolve(auth.server, auth.api_key, config_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = f(target, json).await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
