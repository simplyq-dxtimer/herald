mod handler;
mod poller;
mod streamer;

use herald_cli::{admin, client, config, error};

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
            match admin::register(server, customer_id.clone(), secret).await {
                Ok(v) => render(v, json, |v| {
                    println!("customer_id: {customer_id}");
                    println!("api_key:     {}", v["api_key"].as_str().unwrap_or(""));
                    println!();
                    println!("Store the key now — it is shown in full here and nowhere else.");
                }),
                Err(e) => { eprintln!("{e}"); std::process::exit(1); }
            }
        }

        Commands::Depth { endpoint, auth, json } => {
            run_admin(auth, &config_path, json, |t| admin::depth(t, endpoint), |v| {
                println!("{}: {} queued", v["endpoint"].as_str().unwrap_or(""), v["queue_depth"]);
            }).await;
        }

        Commands::Poll { endpoint, limit, visibility_timeout, auth, json } => {
            run_admin(auth, &config_path, json, |t| admin::poll(t, endpoint, limit, visibility_timeout), |v| {
                let empty = vec![];
                let data = v["data"].as_array().unwrap_or(&empty);
                println!("{}: leased {} of {} queued", v["endpoint"].as_str().unwrap_or(""), data.len(), v["queue_depth"]);
                for m in data {
                    println!("  {}  deliver_count={}  {}",
                        m["message_id"].as_str().unwrap_or(""),
                        m["deliver_count"],
                        m["body"].as_str().unwrap_or("<binary>"));
                }
                if !data.is_empty() {
                    println!();
                    println!("Leased for {}s — ACK them or they redeliver.", v["visibility_timeout"]);
                }
            }).await;
        }

        Commands::Ack { endpoint, message_ids, auth, json } => {
            run_admin(auth, &config_path, json, |t| admin::ack(t, endpoint, message_ids), |v| {
                println!("acked {} message(s)", v["acked"].as_array().map(|a| a.len()).unwrap_or(0));
            }).await;
        }

        Commands::Nack { endpoint, message_id, dlq, auth, json } => {
            run_admin(auth, &config_path, json, |t| admin::nack(t, endpoint, message_id, dlq), |v| {
                println!("{} -> {}", v["message_id"].as_str().unwrap_or(""), v["disposition"].as_str().unwrap_or(""));
                if let Some(w) = v["warning"].as_str() {
                    println!();
                    println!("Note: {w}");
                }
            }).await;
        }

        Commands::Heartbeat { endpoint, message_id, extend, auth, json } => {
            run_admin(auth, &config_path, json, |t| admin::heartbeat(t, endpoint, message_id, extend), |v| {
                println!("{} visibility extended", v["message_id"].as_str().unwrap_or(""));
            }).await;
        }

        Commands::Account { tier, auth, json } => {
            run_admin(auth, &config_path, json, |t| admin::account(t, tier), |v| {
                println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
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

/// Print a result as JSON, or hand it to a human renderer.
fn render(value: serde_json::Value, json: bool, human: impl FnOnce(&serde_json::Value)) {
    if json {
        println!("{}", serde_json::to_string_pretty(&value).unwrap_or_default());
    } else {
        human(&value);
    }
}

/// Resolve the target, run an admin command, render it, and exit non-zero on
/// failure. The operation itself lives in `herald_cli::admin` so `herald-mcp`
/// runs the identical code path.
async fn run_admin<F, Fut>(
    auth: AuthArgs,
    config_path: &PathBuf,
    json: bool,
    f: F,
    human: impl FnOnce(&serde_json::Value),
) where
    F: FnOnce(admin::Target) -> Fut,
    Fut: std::future::Future<Output = Result<serde_json::Value, error::CliError>>,
{
    let target = match admin::resolve(auth.server, auth.api_key, config_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    match f(target).await {
        Ok(v) => render(v, json, human),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}
