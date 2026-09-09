//! MCP server exposing Herald's management API as agent tools.
//!
//! The delivery direction (Herald invokes your agent per webhook) is
//! `herald-cli run`. This is the other direction: an agent creating endpoints,
//! inspecting queues and settling messages without a human editing config.
//!
//! Operations live in `herald_cli::admin`, the same code the CLI runs, so the
//! two front doors cannot drift.
//!
//! Config comes from the environment, because an MCP server is launched by the
//! client and has no argv of its own:
//!
//!   HERALD_SERVER            admin base URL (tailnet, e.g. http://100.90.105.9:8081)
//!   HERALD_API_KEY           account API key
//!   HERALD_REGISTER_SECRET   only needed by herald_register

use std::sync::Arc;

use herald_cli::admin;
use rmcp::handler::server::ServerHandler;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServiceExt};
use serde_json::{json, Map, Value};

#[derive(Clone)]
struct Herald;

/// Build a tool's JSON Schema from a list of (name, type, description) plus the
/// required set.
fn schema(props: &[(&str, &str, &str)], required: &[&str]) -> Arc<Map<String, Value>> {
    let mut p = Map::new();
    for (name, ty, desc) in props {
        p.insert(
            name.to_string(),
            json!({ "type": ty, "description": desc }),
        );
    }
    let mut root = Map::new();
    root.insert("type".into(), json!("object"));
    root.insert("properties".into(), Value::Object(p));
    root.insert("required".into(), json!(required));
    Arc::new(root)
}

fn tool(name: &'static str, description: &'static str, input_schema: Arc<Map<String, Value>>) -> Tool {
    Tool::new(name, description, input_schema)
}

fn arg_str(args: &Map<String, Value>, key: &str) -> Result<String, ErrorData> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| ErrorData::invalid_params(format!("missing required argument: {key}"), None))
}

fn arg_u64(args: &Map<String, Value>, key: &str) -> Option<u64> {
    args.get(key).and_then(|v| v.as_u64())
}

/// Resolve the target from the environment for every call, so a client that
/// rotates HERALD_API_KEY does not need the server restarted.
fn target() -> Result<admin::Target, ErrorData> {
    admin::resolve(None, None, &std::path::PathBuf::from("/nonexistent"))
        .map_err(|e| ErrorData::invalid_params(e.to_string(), None))
}

fn ok(value: Value) -> CallToolResult {
    CallToolResult::success(vec![ContentBlock::text(
        serde_json::to_string_pretty(&value).unwrap_or_default(),
    )])
}

/// Tool-level failure: the call was routed correctly but the operation failed.
/// The caller sees this text; `Err(ErrorData)` would be rendered opaquely.
fn failed(e: impl std::fmt::Display) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(e.to_string())])
}

impl ServerHandler for Herald {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.server_info = Implementation::new("herald-mcp", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "Manage a Herald webhook relay. Point providers at \
             <public-url>/<customer_id>/<endpoint>; messages are read back \
             through these tools. Use herald_queue_depth to look at a queue and \
             herald_poll_messages only when you intend to process: polling \
             leases messages and they redeliver unless acked. Messages that \
             keep failing land in the dead letter queue — herald_dlq_list \
             shows them, herald_dlq_replay retries them."
                .to_string(),
        );
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: vec![
                tool(
                    "herald_register",
                    "Create an account and return its API key. Idempotent: an existing customer_id returns the same key. customer_id must match ^[a-z0-9][a-z0-9-]{2,31}$ and must not be a reserved word (account, endpoints, register, stripe, health, admin, api, ...). Requires HERALD_REGISTER_SECRET if the server gates registration.",
                    schema(&[("customer_id", "string", "Desired customer id")], &["customer_id"]),
                ),
                tool(
                    "herald_queue_depth",
                    "Number of messages waiting on an endpoint. Read-only: it does not lease, redeliver, or alter delivery counts. Use this to check a queue rather than polling.",
                    schema(&[("endpoint", "string", "Endpoint name, e.g. github")], &["endpoint"]),
                ),
                tool(
                    "herald_poll_messages",
                    "Lease and return messages for processing. THIS MUTATES THE QUEUE: leased messages move to in-flight, their deliver_count increments, and they are redelivered unless acked before visibility_timeout expires. Only call this when you intend to process and then ack or nack. To merely inspect a queue, use herald_queue_depth.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("limit", "integer", "Max messages to lease (default 10, server caps at 100)"),
                            ("visibility_timeout", "integer", "Seconds before an unacked message redelivers (default 300)"),
                        ],
                        &["endpoint"],
                    ),
                ),
                tool(
                    "herald_ack",
                    "Acknowledge processed messages, removing them permanently. Pass several message_ids to settle them in one request.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("message_ids", "array", "Message ids returned by herald_poll_messages"),
                        ],
                        &["endpoint", "message_ids"],
                    ),
                ),
                tool(
                    "herald_nack",
                    "Return a message to the queue for another attempt, or send it to the dead letter queue with dlq=true. WARNING: this server exposes no route to read or drain the DLQ, so dlq=true is a one-way door — the payload cannot be retrieved through the API afterwards.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("message_id", "string", "Message id"),
                            ("dlq", "boolean", "Send to the dead letter queue instead of requeueing (default false)"),
                        ],
                        &["endpoint", "message_id"],
                    ),
                ),
                tool(
                    "herald_heartbeat",
                    "Extend the visibility timeout on an in-flight message. Call this when processing legitimately takes longer than the lease, to stop the message being redelivered while still being worked on.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("message_id", "string", "Message id"),
                            ("extend", "integer", "Additional seconds"),
                        ],
                        &["endpoint", "message_id"],
                    ),
                ),
                tool(
                    "herald_dlq_list",
                    "Read the dead letter queue: messages that exhausted their retries or were explicitly nacked to the DLQ. Read-only — listing never replays, purges, or changes delivery counts, so it is safe to call while diagnosing. Entries marked expired:true are ids whose payload aged out of retention; they can only be purged, not replayed.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("offset", "integer", "Start offset from the most recently dead message (default 0)"),
                            ("limit", "integer", "Max entries (default 50, capped at 100)"),
                        ],
                        &["endpoint"],
                    ),
                ),
                tool(
                    "herald_dlq_replay",
                    "Move dead messages back onto the main queue for another attempt. Pass message_id to replay one, or omit it to replay the whole DLQ. deliver_count is reset, so a replayed message gets a full set of retries rather than dying again on first failure. Replayed messages go to the back of the queue and do not jump ahead of live webhooks. Fix the cause before replaying, or they will simply fail again.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("message_id", "string", "Message to replay; omit to replay every dead message"),
                        ],
                        &["endpoint"],
                    ),
                ),
                tool(
                    "herald_dlq_purge",
                    "Permanently delete dead messages and their payloads. Pass message_id to purge one, or omit it to purge the entire DLQ. THIS CANNOT BE UNDONE and the payload is not recoverable afterwards — list the DLQ first and confirm with the user before purging everything. Purging does not clear the dedup fingerprint, so an identical payload re-sent later is still deduplicated.",
                    schema(
                        &[
                            ("endpoint", "string", "Endpoint name"),
                            ("message_id", "string", "Message to purge; omit to purge every dead message"),
                        ],
                        &["endpoint"],
                    ),
                ),
                tool(
                    "herald_account",
                    "Read account and billing state, or change the tier by passing one of free, standard, pro, enterprise. Tier governs endpoint count, daily message allowance, payload size, retention and whether WebSocket streaming is permitted.",
                    schema(&[("tier", "string", "Optional new tier; omit to read current state")], &[]),
                ),
            ],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let args = request.arguments.unwrap_or_default();
        let name = request.name.as_ref();

        let result = match name {
            "herald_register" => {
                let customer_id = arg_str(&args, "customer_id")?;
                let server = admin::resolve_server(None, &std::path::PathBuf::from("/nonexistent"))
                    .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?;
                admin::register(server, customer_id, None).await
            }
            "herald_queue_depth" => admin::depth(target()?, arg_str(&args, "endpoint")?).await,
            "herald_poll_messages" => {
                admin::poll(
                    target()?,
                    arg_str(&args, "endpoint")?,
                    arg_u64(&args, "limit").unwrap_or(10) as usize,
                    arg_u64(&args, "visibility_timeout").unwrap_or(300),
                )
                .await
            }
            "herald_ack" => {
                let ids: Vec<String> = args
                    .get("message_ids")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                if ids.is_empty() {
                    return Err(ErrorData::invalid_params(
                        "message_ids must be a non-empty array of strings",
                        None,
                    ));
                }
                admin::ack(target()?, arg_str(&args, "endpoint")?, ids).await
            }
            "herald_nack" => {
                admin::nack(
                    target()?,
                    arg_str(&args, "endpoint")?,
                    arg_str(&args, "message_id")?,
                    args.get("dlq").and_then(|v| v.as_bool()).unwrap_or(false),
                )
                .await
            }
            "herald_heartbeat" => {
                admin::heartbeat(
                    target()?,
                    arg_str(&args, "endpoint")?,
                    arg_str(&args, "message_id")?,
                    arg_u64(&args, "extend"),
                )
                .await
            }
            "herald_dlq_list" => {
                admin::dlq_list(
                    target()?,
                    arg_str(&args, "endpoint")?,
                    args.get("offset").and_then(|v| v.as_i64()).unwrap_or(0),
                    args.get("limit").and_then(|v| v.as_i64()).unwrap_or(50),
                )
                .await
            }
            "herald_dlq_replay" => {
                admin::dlq_replay(
                    target()?,
                    arg_str(&args, "endpoint")?,
                    args.get("message_id").and_then(|v| v.as_str()).map(String::from),
                )
                .await
            }
            "herald_dlq_purge" => {
                admin::dlq_purge(
                    target()?,
                    arg_str(&args, "endpoint")?,
                    args.get("message_id").and_then(|v| v.as_str()).map(String::from),
                )
                .await
            }
            "herald_account" => {
                let tier = args.get("tier").and_then(|v| v.as_str()).map(String::from);
                admin::account(target()?, tier).await
            }
            other => {
                return Err(ErrorData::invalid_params(
                    format!("unknown tool: {other}"),
                    None,
                ))
            }
        };

        Ok(match result {
            Ok(v) => ok(v),
            Err(e) => failed(e),
        }
        .into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // stderr only: stdout is the MCP transport and anything else on it is a
    // protocol violation.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let service = Herald.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}
