//! Integration test harness for the Predict adapter (offline fixture
//! tests plus local-peer protocol tests; live connections are acceptance
//! work, not default-track tests).

#[path = "integration/data_client.rs"]
mod data_client;

#[path = "integration/http.rs"]
mod http;

#[path = "integration/ws.rs"]
mod ws;
