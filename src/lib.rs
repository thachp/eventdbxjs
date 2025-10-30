//! Node.js bindings for the EventDBX control socket.
//!
//! This addon wraps the async `ControlClient` exposed from the bundled plugin API
//! and translates the JSON payloads into JavaScript objects. Consumers can
//! list aggregates, fetch a single aggregate, enumerate events, append a new
//! event, or apply a JSON Patch against the current state.

use std::str::FromStr;
use std::sync::Arc;

pub mod plugin_api;

use crate::plugin_api::{
  AggregateSortFieldSpec, AggregateSortSpec, AggregateStateView, AppendEventRequest, ControlClient,
  ControlClientError, CreateAggregateRequest, FilterExpressionSpec, PatchEventRequest,
  SetAggregateArchiveRequest, StoredEventRecord,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
struct ClientConfig {
  ip: String,
  port: u16,
  token: Option<String>,
}

impl Default for ClientConfig {
  fn default() -> Self {
    Self {
      ip: std::env::var("EVENTDBX_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
      port: std::env::var("EVENTDBX_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(6363),
      token: std::env::var("EVENTDBX_TOKEN")
        .ok()
        .filter(|value| !value.is_empty()),
    }
  }
}

impl ClientConfig {
  fn endpoint(&self) -> String {
    format!("{}:{}", self.ip, self.port)
  }
}

#[derive(Default)]
struct ClientState {
  client: Option<ControlClient>,
}

#[napi(object)]
pub struct ClientOptions {
  pub ip: Option<String>,
  pub port: Option<u16>,
  pub token: Option<String>,
}

impl From<ClientOptions> for ClientConfig {
  fn from(options: ClientOptions) -> Self {
    let mut config = ClientConfig::default();
    if let Some(ip) = options.ip {
      config.ip = ip;
    }
    if let Some(port) = options.port {
      config.port = port;
    }
    if let Some(token) = options.token {
      config.token = Some(token);
    }
    config
  }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[napi(object)]
pub struct PageOptions {
  pub take: Option<u32>,
  pub skip: Option<u32>,
  pub include_archived: Option<bool>,
  pub archived_only: Option<bool>,
  pub token: Option<String>,
  pub filter: Option<JsonValue>,
  pub sort: Option<Vec<AggregateSortInput>>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[napi(object)]
pub struct AppendOptions {
  pub payload: Option<serde_json::Value>,
  pub metadata: Option<serde_json::Value>,
  pub note: Option<String>,
  pub token: Option<String>,
}

#[napi(object)]
pub struct PatchOptions {
  pub metadata: Option<serde_json::Value>,
  pub note: Option<String>,
  pub token: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[napi(object)]
pub struct CreateAggregateOptions {
  pub token: Option<String>,
  pub payload: Option<serde_json::Value>,
  pub metadata: Option<serde_json::Value>,
  pub note: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[napi(object)]
pub struct SetArchiveOptions {
  pub token: Option<String>,
  pub comment: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[napi(object)]
pub struct AggregateSortInput {
  pub field: String,
  pub descending: Option<bool>,
}

#[napi]
pub struct DbxClient {
  config: ClientConfig,
  state: Arc<Mutex<ClientState>>,
}

#[napi]
impl DbxClient {
  #[napi(constructor)]
  pub fn new(options: Option<ClientOptions>) -> Self {
    let config = options.map(Into::into).unwrap_or_default();
    Self {
      config,
      state: Arc::new(Mutex::new(ClientState::default())),
    }
  }

  /// Establish a TCP connection to the EventDBX control socket.
  #[napi]
  pub async fn connect(&self) -> napi::Result<()> {
    let mut guard = self.state.lock().await;
    if guard.client.is_some() {
      return Ok(());
    }

    let addr = self.config.endpoint();
    let token = self
      .config
      .token
      .clone()
      .filter(|value| !value.trim().is_empty())
      .ok_or_else(|| {
        napi_err(
          Status::InvalidArg,
          "control token must be provided via client options or EVENTDBX_TOKEN",
        )
      })?;
    let client = ControlClient::connect(&addr, token.as_str())
      .await
      .map_err(control_err_to_napi)?;
    guard.client = Some(client);
    Ok(())
  }

  /// Close the underlying socket, if connected.
  #[napi]
  pub async fn disconnect(&self) -> napi::Result<()> {
    let mut guard = self.state.lock().await;
    guard.client.take();
    Ok(())
  }

  /// Returns `true` if a socket connection is currently held.
  #[napi]
  pub async fn is_connected(&self) -> napi::Result<bool> {
    let guard = self.state.lock().await;
    Ok(guard.client.is_some())
  }

  /// Returns the configured endpoint (host/port).
  #[napi(getter)]
  pub fn endpoint(&self) -> ClientEndpoint {
    ClientEndpoint {
      ip: self.config.ip.clone(),
      port: self.config.port,
    }
  }

  /// List aggregates, optionally restricting to a specific aggregate type.
  #[napi(js_name = "list")]
  pub async fn list_aggregates(
    &self,
    aggregate_type: Option<String>,
    options: Option<PageOptions>,
  ) -> napi::Result<Vec<serde_json::Value>> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let skip = options.as_ref().and_then(|o| o.skip).unwrap_or(0) as usize;
    let take = options.as_ref().and_then(|o| o.take.map(|v| v as usize));
    let archived_only = options
      .as_ref()
      .and_then(|o| o.archived_only)
      .unwrap_or(false);
    let include_archived = options
      .as_ref()
      .and_then(|o| o.include_archived)
      .unwrap_or(false)
      || archived_only;
    let filter_spec = options
      .as_ref()
      .and_then(|o| o.filter.as_ref())
      .map(|value| serde_json::from_value::<FilterExpressionSpec>(value.clone()))
      .transpose()
      .map_err(|err| {
        napi_err(
          Status::InvalidArg,
          format!("invalid filter expression: {err}"),
        )
      })?;
    let sort_specs = options
      .as_ref()
      .and_then(|o| o.sort.as_ref())
      .map(|entries| {
        let mut specs = Vec::with_capacity(entries.len());
        for entry in entries {
          let field = AggregateSortFieldSpec::from_str(entry.field.as_str()).map_err(|_| {
            napi_err(
              Status::InvalidArg,
              format!("unknown aggregate sort field '{}'", entry.field),
            )
          })?;
          specs.push(AggregateSortSpec {
            field,
            descending: entry.descending.unwrap_or(false),
          });
        }
        Ok::<Vec<AggregateSortSpec>, napi::Error>(specs)
      })
      .transpose()?
      .unwrap_or_default();
    let token = options
      .as_ref()
      .and_then(|o| o.token.clone())
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let aggregates = client
      .list_aggregates(
        token.as_str(),
        skip,
        take,
        include_archived,
        archived_only,
        filter_spec.as_ref(),
        sort_specs.as_slice(),
      )
      .await
      .map_err(control_err_to_napi)?;

    let filter = aggregate_type.as_deref();
    let result = aggregates
      .into_iter()
      .filter(|agg| match filter {
        Some(kind) => agg.aggregate_type == kind,
        None => true,
      })
      .map(aggregate_to_json)
      .collect();

    Ok(result)
  }

  /// Fetch a single aggregate snapshot.
  #[napi(js_name = "get")]
  pub async fn get_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
  ) -> napi::Result<Option<serde_json::Value>> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let token = self.config.token.clone().unwrap_or_default();
    let aggregate = client
      .get_aggregate(token.as_str(), &aggregate_type, &aggregate_id)
      .await
      .map_err(control_err_to_napi)?;

    Ok(aggregate.map(aggregate_to_json))
  }

  /// Select a subset of fields from an aggregate snapshot.
  #[napi(js_name = "select")]
  pub async fn select_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    fields: Vec<String>,
  ) -> napi::Result<Option<serde_json::Value>> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let token = self.config.token.clone().unwrap_or_default();

    let selection = client
      .select_aggregate(token.as_str(), &aggregate_type, &aggregate_id, &fields)
      .await
      .map_err(control_err_to_napi)?;

    Ok(selection)
  }

  /// List events for an aggregate.
  #[napi(js_name = "events")]
  pub async fn list_events(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    options: Option<PageOptions>,
  ) -> napi::Result<Vec<serde_json::Value>> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let skip = options.as_ref().and_then(|o| o.skip).unwrap_or(0) as usize;
    let take = options.as_ref().and_then(|o| o.take.map(|v| v as usize));
    let filter_spec = options
      .as_ref()
      .and_then(|o| o.filter.as_ref())
      .map(|value| serde_json::from_value::<FilterExpressionSpec>(value.clone()))
      .transpose()
      .map_err(|err| {
        napi_err(
          Status::InvalidArg,
          format!("invalid filter expression: {err}"),
        )
      })?;
    let token = options
      .as_ref()
      .and_then(|o| o.token.clone())
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let events = client
      .list_events(
        token.as_str(),
        &aggregate_type,
        &aggregate_id,
        skip,
        take,
        filter_spec.as_ref(),
      )
      .await
      .map_err(control_err_to_napi)?;

    Ok(events.into_iter().map(event_to_json).collect())
  }

  /// Append a new event with an arbitrary JSON payload.
  #[napi(js_name = "apply")]
  pub async fn append_event(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    options: Option<AppendOptions>,
  ) -> napi::Result<serde_json::Value> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let opts = options.unwrap_or_default();
    let AppendOptions {
      payload,
      metadata,
      note,
      token,
    } = opts;

    let request = AppendEventRequest {
      token: token
        .or_else(|| self.config.token.clone())
        .unwrap_or_default(),
      aggregate_type,
      aggregate_id,
      event_type,
      payload,
      metadata,
      note,
    };

    let record = client
      .append_event(request)
      .await
      .map_err(control_err_to_napi)?;

    Ok(event_to_json(record))
  }

  /// Create an aggregate and emit its initial event.
  #[napi(js_name = "create")]
  pub async fn create_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    options: Option<CreateAggregateOptions>,
  ) -> napi::Result<serde_json::Value> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let opts = options.unwrap_or_default();
    let CreateAggregateOptions {
      token,
      payload,
      metadata,
      note,
    } = opts;
    let token = token
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let request = CreateAggregateRequest {
      token,
      aggregate_type,
      aggregate_id,
      event_type,
      payload,
      metadata,
      note,
    };

    let aggregate = client
      .create_aggregate(request)
      .await
      .map_err(control_err_to_napi)?;

    Ok(aggregate_to_json(aggregate))
  }

  async fn set_archive_state(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    archived: bool,
    options: Option<SetArchiveOptions>,
  ) -> napi::Result<serde_json::Value> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let opts = options.unwrap_or_default();
    let SetArchiveOptions { token, comment } = opts;
    let token = token
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let request = SetAggregateArchiveRequest {
      token,
      aggregate_type,
      aggregate_id,
      archived,
      comment,
    };

    let aggregate = client
      .set_aggregate_archive(request)
      .await
      .map_err(control_err_to_napi)?;

    Ok(aggregate_to_json(aggregate))
  }

  /// Archive an aggregate.
  #[napi(js_name = "archive")]
  pub async fn archive_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    options: Option<SetArchiveOptions>,
  ) -> napi::Result<serde_json::Value> {
    self
      .set_archive_state(aggregate_type, aggregate_id, true, options)
      .await
  }

  /// Restore an archived aggregate.
  #[napi(js_name = "restore")]
  pub async fn restore_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    options: Option<SetArchiveOptions>,
  ) -> napi::Result<serde_json::Value> {
    self
      .set_archive_state(aggregate_type, aggregate_id, false, options)
      .await
  }

  /// Apply a JSON Patch to the aggregate. Returns the updated snapshot.
  #[napi(js_name = "patch")]
  pub async fn patch_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    operations: Vec<serde_json::Value>,
    options: Option<PatchOptions>,
  ) -> napi::Result<serde_json::Value> {
    let mut guard = self.state.lock().await;
    let client = guard
      .client
      .as_mut()
      .ok_or_else(|| napi_err(Status::GenericFailure, "client is not connected"))?;
    let opts = options.unwrap_or(PatchOptions {
      metadata: None,
      note: None,
      token: None,
    });
    let PatchOptions {
      metadata,
      note,
      token,
    } = opts;
    let token = token
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let patch = JsonValue::Array(operations);
    let request = PatchEventRequest {
      token: token.clone(),
      aggregate_type: aggregate_type.clone(),
      aggregate_id: aggregate_id.clone(),
      event_type,
      patch,
      metadata,
      note,
    };

    client
      .patch_event(request)
      .await
      .map_err(control_err_to_napi)?;

    let aggregate = client
      .get_aggregate(token.as_str(), &aggregate_type, &aggregate_id)
      .await
      .map_err(control_err_to_napi)?
      .ok_or_else(|| napi_err(Status::GenericFailure, "aggregate not found after patch"))?;

    Ok(aggregate_to_json(aggregate))
  }
}

#[napi]
pub fn create_client(options: Option<ClientOptions>) -> DbxClient {
  DbxClient::new(options)
}

#[napi(object)]
pub struct ClientEndpoint {
  pub ip: String,
  pub port: u16,
}

fn aggregate_to_json(view: AggregateStateView) -> serde_json::Value {
  let mut state_map = JsonMap::new();
  for (key, raw_value) in view.state {
    state_map.insert(key, parse_state_value(&raw_value));
  }

  json!({
      "aggregateType": view.aggregate_type,
      "aggregateId": view.aggregate_id,
      "version": view.version,
      "state": JsonValue::Object(state_map),
      "merkleRoot": view.merkle_root,
      "archived": view.archived,
  })
}

fn parse_state_value(raw: &str) -> JsonValue {
  serde_json::from_str(raw).unwrap_or_else(|_| JsonValue::String(raw.to_string()))
}

fn event_to_json(record: StoredEventRecord) -> JsonValue {
  serde_json::to_value(record).unwrap_or_else(|_| JsonValue::Null)
}

fn control_err_to_napi(err: ControlClientError) -> napi::Error {
  match err {
    ControlClientError::Io(inner) => napi_err(Status::GenericFailure, inner.to_string()),
    ControlClientError::Capnp(inner) => napi_err(Status::GenericFailure, inner.to_string()),
    ControlClientError::Server { code, message } => napi_err(
      Status::GenericFailure,
      format!("server error ({code}): {message}"),
    ),
    ControlClientError::Protocol(msg) => napi_err(Status::GenericFailure, msg),
    ControlClientError::Json(inner) => napi_err(Status::GenericFailure, inner.to_string()),
  }
}

fn napi_err(status: Status, reason: impl Into<String>) -> napi::Error {
  napi::Error::new(status, reason.into())
}

#[cfg(test)]
mod tests {
  use super::*;
  use chrono::Utc;
  use std::collections::BTreeMap;

  fn sample_aggregate() -> AggregateStateView {
    AggregateStateView {
      aggregate_type: "person".into(),
      aggregate_id: "p-001".into(),
      version: 7,
      state: BTreeMap::from([
        ("name".into(), "\"Jane Doe\"".into()),
        ("age".into(), "42".into()),
        ("active".into(), "true".into()),
        ("notes".into(), "[\"vip\",\"beta\"]".into()),
      ]),
      merkle_root: "abc123".into(),
      archived: false,
    }
  }

  #[test]
  fn aggregate_to_json_flattens_and_parses_values() {
    let view = sample_aggregate();
    let json = aggregate_to_json(view);
    let obj = json.as_object().expect("expected object");

    assert_eq!(obj.get("aggregateType").unwrap(), "person");
    assert_eq!(obj.get("aggregateId").unwrap(), "p-001");
    assert_eq!(obj.get("version").unwrap(), 7);
    assert_eq!(obj.get("merkleRoot").unwrap(), "abc123");
    assert_eq!(obj.get("archived").unwrap(), &JsonValue::Bool(false));

    let state = obj
      .get("state")
      .and_then(JsonValue::as_object)
      .expect("state should be object");
    assert_eq!(state.get("name").unwrap(), "Jane Doe");
    assert_eq!(state.get("age").unwrap(), 42);
    assert_eq!(state.get("active").unwrap(), &JsonValue::Bool(true));
    assert_eq!(state.get("notes").unwrap(), &json!(["vip", "beta"]));
  }

  #[test]
  fn parse_state_value_handles_invalid_json_as_string() {
    let value = parse_state_value("not-json");
    assert_eq!(value, JsonValue::String("not-json".into()));
  }

  #[test]
  fn event_to_json_round_trips_record() {
    let record = StoredEventRecord {
      aggregate_type: "order".into(),
      aggregate_id: "o-1".into(),
      event_type: "order_created".into(),
      version: 1,
      payload: json!({"total": 99}),
      metadata: crate::plugin_api::EventMetadataView {
        event_id: "123".into(),
        created_at: Utc::now(),
        issued_by: None,
        note: None,
      },
      hash: "hash".into(),
      merkle_root: "root".into(),
    };

    let json = event_to_json(record.clone());
    let round_trip: StoredEventRecord = serde_json::from_value(json).expect("round trip");
    assert_eq!(round_trip.aggregate_type, record.aggregate_type);
    assert_eq!(round_trip.payload, record.payload);
    assert_eq!(round_trip.hash, record.hash);
  }
}
