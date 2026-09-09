//! Shared library behind both front doors.
//!
//! `herald-cli` (the binary) and `herald-mcp` both call [`admin`], so a change
//! to an operation reaches the CLI and the MCP tools at once. Neither wraps the
//! other by shelling out — there is one implementation and two renderings of
//! its output.

pub mod admin;
pub mod client;
pub mod config;
pub mod error;
