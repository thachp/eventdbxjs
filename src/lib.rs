//! Node.js bindings for the EventDBX control socket.
//!
//! This addon wraps the async `ControlClient` exposed from the bundled plugin API
//! and translates the JSON payloads into JavaScript objects. Consumers can
//! list aggregates, fetch a single aggregate, enumerate events, append a new
//! event, or apply a JSON Patch against the current state.

use futures::future::BoxFuture;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub mod plugin_api;

use crate::plugin_api::{
  AggregateSortFieldSpec, AggregateSortSpec, AggregateStateView, AppendEventRequest, ControlClient,
  ControlClientError, ControlResult, CreateAggregateRequest, PatchEventRequest,
  SetAggregateArchiveRequest, StoredEventRecord,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Clone, Debug)]
struct ClientConfig {
  ip: String,
  port: u16,
  token: Option<String>,
  tenant_id: Option<String>,
  verbose: bool,
  retry: RetryConfig,
}

const MISSING_TOKEN_MESSAGE: &str =
  "control token must be provided via client options or EVENTDBX_TOKEN";
const CLIENT_NOT_CONNECTED_MESSAGE: &str = "client is not connected";

#[derive(Clone, Debug)]
struct RetryConfig {
  max_attempts: u32,
  initial_delay_ms: u64,
  max_delay_ms: u64,
}

impl Default for RetryConfig {
  fn default() -> Self {
    Self {
      max_attempts: 1,
      initial_delay_ms: 50,
      max_delay_ms: 1_000,
    }
  }
}

impl RetryConfig {
  fn attempts(&self) -> u32 {
    self.max_attempts.max(1)
  }

  fn delay_for_attempt(&self, attempt_number: u32) -> Duration {
    if attempt_number <= 1 || self.initial_delay_ms == 0 {
      return Duration::from_millis(0);
    }

    let exponent = attempt_number.saturating_sub(2).min(31);
    let multiplier = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let scaled = self.initial_delay_ms.saturating_mul(multiplier);
    Duration::from_millis(scaled.min(self.max_delay_ms))
  }
}

#[derive(Clone, Debug)]
struct HandshakeParams {
  endpoint: String,
  token: String,
  tenant_id: Option<String>,
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
      tenant_id: std::env::var("EVENTDBX_TENANT_ID")
        .ok()
        .filter(|value| !value.is_empty()),
      verbose: std::env::var("EVENTDBX_VERBOSE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false),
      retry: RetryConfig::default(),
    }
  }
}

impl ClientConfig {
  fn endpoint(&self) -> String {
    format!("{}:{}", self.ip, self.port)
  }

  fn handshake_params(&self) -> Option<HandshakeParams> {
    let token = self.token.as_ref()?.trim();
    if token.is_empty() {
      return None;
    }

    let tenant_id = self
      .tenant_id
      .as_ref()
      .map(|value| value.trim().to_owned())
      .filter(|value| !value.is_empty());

    Some(HandshakeParams {
      endpoint: self.endpoint(),
      token: token.to_string(),
      tenant_id,
    })
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
  #[napi(js_name = "tenantId")]
  pub tenant_id: Option<String>,
  pub verbose: Option<bool>,
  pub retry: Option<RetryOptions>,
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
    if let Some(tenant) = options.tenant_id {
      config.tenant_id = Some(tenant);
    }
    if let Some(verbose) = options.verbose {
      config.verbose = verbose;
    }
    if let Some(retry) = options.retry {
      config.retry = retry.into();
    }
    config
  }
}

#[derive(Default)]
#[napi(object)]
pub struct RetryOptions {
  pub attempts: Option<u32>,
  #[napi(js_name = "initialDelayMs")]
  pub initial_delay_ms: Option<u32>,
  #[napi(js_name = "maxDelayMs")]
  pub max_delay_ms: Option<u32>,
}

impl From<RetryOptions> for RetryConfig {
  fn from(options: RetryOptions) -> Self {
    let mut config = RetryConfig::default();
    if let Some(attempts) = options.attempts {
      config.max_attempts = attempts.max(1);
    }
    if let Some(initial_delay_ms) = options.initial_delay_ms {
      config.initial_delay_ms = u64::from(initial_delay_ms);
    }
    if let Some(max_delay_ms) = options.max_delay_ms {
      config.max_delay_ms = u64::from(max_delay_ms);
    }
    if config.max_delay_ms < config.initial_delay_ms {
      config.max_delay_ms = config.initial_delay_ms;
    }
    config
  }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[napi(object)]
pub struct PageOptions {
  pub cursor: Option<String>,
  pub take: Option<u32>,
  pub include_archived: Option<bool>,
  pub archived_only: Option<bool>,
  pub token: Option<String>,
  pub filter: Option<String>,
  pub sort: Option<Vec<AggregateSortInput>>,
}

#[napi(object)]
pub struct PageResult {
  pub items: Vec<JsonValue>,
  pub next_cursor: Option<String>,
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
  pub note: Option<String>,
  #[serde(rename = "comment")]
  #[napi(js_name = "comment")]
  pub legacy_comment: Option<String>,
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
    let params = self
      .config
      .handshake_params()
      .ok_or_else(|| napi_err(Status::InvalidArg, MISSING_TOKEN_MESSAGE))?;
    let endpoint = params.endpoint;
    let token = params.token;
    let tenant_id = params.tenant_id;

    let mut guard = self.state.lock().await;
    if guard.client.is_some() {
      return Ok(());
    }

    let max_attempts = self.config.retry.attempts();
    let mut attempt = 0;

    loop {
      attempt += 1;
      match ControlClient::connect_with_tenant(&endpoint, token.as_str(), tenant_id.as_deref())
        .await
      {
        Ok(client) => {
          guard.client = Some(client);
          return Ok(());
        }
        Err(err) => {
          if attempt >= max_attempts {
            return Err(control_err_to_napi(err));
          }

          let delay = self.config.retry.delay_for_attempt(attempt + 1);
          drop(guard);
          if delay.as_millis() > 0 {
            sleep(delay).await;
          }
          guard = self.state.lock().await;
          if guard.client.is_some() {
            return Ok(());
          }
        }
      }
    }
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
  ) -> napi::Result<PageResult> {
    let cursor = options
      .as_ref()
      .and_then(|o| o.cursor.as_ref())
      .map(|value| value.trim().to_owned())
      .filter(|value| !value.is_empty());
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
    let filter = options
      .as_ref()
      .and_then(|o| o.filter.as_ref())
      .map(|value| value.trim().to_owned())
      .filter(|value| !value.is_empty());
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
    let page = self
      .call_with_retry({
        let cursor = cursor;
        let filter = filter;
        let token = token;
        let sort_specs = sort_specs;
        move |client| {
          let token = token.clone();
          let cursor = cursor.clone();
          let filter = filter.clone();
          let sort_specs = sort_specs.clone();
          Box::pin(async move {
            client
              .list_aggregates(
                token.as_str(),
                cursor.as_deref(),
                take,
                include_archived,
                archived_only,
                filter.as_deref(),
                sort_specs.as_slice(),
              )
              .await
          })
        }
      })
      .await
      .map_err(control_err_to_napi)?;

    let filter = aggregate_type.as_deref();
    let items = page
      .items
      .into_iter()
      .filter(|agg| match filter {
        Some(kind) => agg.aggregate_type == kind,
        None => true,
      })
      .map(aggregate_to_json)
      .collect();

    Ok(PageResult {
      items,
      next_cursor: page.next_cursor,
    })
  }

  /// Fetch a single aggregate snapshot.
  #[napi(js_name = "get")]
  pub async fn get_aggregate(
    &self,
    aggregate_type: String,
    aggregate_id: String,
  ) -> napi::Result<Option<serde_json::Value>> {
    let token = self.config.token.clone().unwrap_or_default();
    let aggregate = self
      .call_with_retry({
        let token = token;
        let aggregate_type = aggregate_type;
        let aggregate_id = aggregate_id;
        move |client| {
          let token = token.clone();
          let aggregate_type = aggregate_type.clone();
          let aggregate_id = aggregate_id.clone();
          Box::pin(async move {
            client
              .get_aggregate(token.as_str(), &aggregate_type, &aggregate_id)
              .await
          })
        }
      })
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
    let token = self.config.token.clone().unwrap_or_default();

    let selection = self
      .call_with_retry({
        let token = token;
        let aggregate_type = aggregate_type;
        let aggregate_id = aggregate_id;
        let fields = fields;
        move |client| {
          let token = token.clone();
          let aggregate_type = aggregate_type.clone();
          let aggregate_id = aggregate_id.clone();
          let fields = fields.clone();
          Box::pin(async move {
            client
              .select_aggregate(token.as_str(), &aggregate_type, &aggregate_id, &fields)
              .await
          })
        }
      })
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
  ) -> napi::Result<PageResult> {
    let cursor = options
      .as_ref()
      .and_then(|o| o.cursor.as_ref())
      .map(|value| value.trim().to_owned())
      .filter(|value| !value.is_empty());
    let take = options.as_ref().and_then(|o| o.take.map(|v| v as usize));
    let filter = options
      .as_ref()
      .and_then(|o| o.filter.as_ref())
      .map(|value| value.trim().to_owned())
      .filter(|value| !value.is_empty());
    let token = options
      .as_ref()
      .and_then(|o| o.token.clone())
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let page = self
      .call_with_retry({
        let token = token;
        let aggregate_type = aggregate_type;
        let aggregate_id = aggregate_id;
        let cursor = cursor;
        let filter = filter;
        move |client| {
          let token = token.clone();
          let aggregate_type = aggregate_type.clone();
          let aggregate_id = aggregate_id.clone();
          let cursor = cursor.clone();
          let filter = filter.clone();
          Box::pin(async move {
            client
              .list_events(
                token.as_str(),
                &aggregate_type,
                &aggregate_id,
                cursor.as_deref(),
                take,
                filter.as_deref(),
              )
              .await
          })
        }
      })
      .await
      .map_err(control_err_to_napi)?;

    Ok(PageResult {
      items: page.items.into_iter().map(event_to_json).collect(),
      next_cursor: page.next_cursor,
    })
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
    let opts = options.unwrap_or_default();
    let AppendOptions {
      payload,
      metadata,
      note,
      token,
    } = opts;

    let token = token
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();

    let request = AppendEventRequest {
      token,
      aggregate_type,
      aggregate_id,
      event_type,
      payload,
      metadata,
      note,
    };

    match self
      .call_with_retry({
        let request = request.clone();
        move |client| {
          let req = request.clone();
          Box::pin(async move { client.append_event(req).await })
        }
      })
      .await
    {
      Ok(record) => Ok(event_to_json(record)),
      Err(err) => {
        if self.should_treat_as_ok("appendEvent", &err) {
          Ok(ok_response_value())
        } else {
          Err(control_err_to_napi(err))
        }
      }
    }
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

    match self
      .call_with_retry({
        let request = request.clone();
        move |client| {
          let req = request.clone();
          Box::pin(async move { client.create_aggregate(req).await })
        }
      })
      .await
    {
      Ok(aggregate) => Ok(aggregate_to_json(aggregate)),
      Err(err) => {
        if self.should_treat_as_ok("createAggregate", &err) {
          Ok(ok_response_value())
        } else {
          Err(control_err_to_napi(err))
        }
      }
    }
  }

  async fn set_archive_state(
    &self,
    aggregate_type: String,
    aggregate_id: String,
    archived: bool,
    options: Option<SetArchiveOptions>,
  ) -> napi::Result<serde_json::Value> {
    let opts = options.unwrap_or_default();
    let SetArchiveOptions {
      token,
      note,
      legacy_comment,
    } = opts;
    let token = token
      .or_else(|| self.config.token.clone())
      .unwrap_or_default();
    let note = note.or(legacy_comment);

    let request = SetAggregateArchiveRequest {
      token,
      aggregate_type,
      aggregate_id,
      archived,
      note,
    };

    match self
      .call_with_retry({
        let request = request.clone();
        move |client| {
          let req = request.clone();
          Box::pin(async move { client.set_aggregate_archive(req).await })
        }
      })
      .await
    {
      Ok(aggregate) => Ok(aggregate_to_json(aggregate)),
      Err(err) => {
        if self.should_treat_as_ok("setAggregateArchive", &err) {
          Ok(ok_response_value())
        } else {
          Err(control_err_to_napi(err))
        }
      }
    }
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
    #[napi(ts_arg_type = "JsonPatch[]")] operations: Vec<serde_json::Value>,
    options: Option<PatchOptions>,
  ) -> napi::Result<serde_json::Value> {
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
    let aggregate_type_for_fetch = aggregate_type.clone();
    let aggregate_id_for_fetch = aggregate_id.clone();
    let request = PatchEventRequest {
      token: token.clone(),
      aggregate_type: aggregate_type.clone(),
      aggregate_id: aggregate_id.clone(),
      event_type,
      patch,
      metadata,
      note,
    };

    match self
      .call_with_retry({
        let request = request.clone();
        move |client| {
          let req = request.clone();
          Box::pin(async move { client.patch_event(req).await })
        }
      })
      .await
    {
      Ok(_) => match self
        .call_with_retry({
          let token = token.clone();
          move |client| {
            let token = token.clone();
            let aggregate_type = aggregate_type_for_fetch.clone();
            let aggregate_id = aggregate_id_for_fetch.clone();
            Box::pin(async move {
              client
                .get_aggregate(token.as_str(), &aggregate_type, &aggregate_id)
                .await
            })
          }
        })
        .await
      {
        Ok(Some(aggregate)) => Ok(aggregate_to_json(aggregate)),
        Ok(None) => {
          if self.config.verbose {
            Err(napi_err(
              Status::GenericFailure,
              "aggregate not found after patch",
            ))
          } else {
            Ok(ok_response_value())
          }
        }
        Err(err) => Err(control_err_to_napi(err)),
      },
      Err(err) => {
        if self.should_treat_as_ok("patchEvent", &err) {
          Ok(ok_response_value())
        } else {
          Err(control_err_to_napi(err))
        }
      }
    }
  }

  fn should_treat_as_ok(&self, operation: &str, err: &ControlClientError) -> bool {
    if self.config.verbose {
      return false;
    }

    match err {
      ControlClientError::Protocol(message) => {
        let msg_lower = message.to_ascii_lowercase();
        let op_lower = operation.to_ascii_lowercase();
        msg_lower.contains("unexpected response") && msg_lower.contains(&op_lower)
      }
      ControlClientError::Json(_) => true,
      _ => false,
    }
  }

  async fn call_with_retry<F, T>(&self, mut action: F) -> ControlResult<T>
  where
    F: FnMut(&mut ControlClient) -> BoxFuture<'_, ControlResult<T>> + Send,
    T: Send,
  {
    let max_attempts = self.config.retry.attempts();
    let mut require_connected = true;

    for attempt in 1..=max_attempts {
      if require_connected {
        let guard = self.state.lock().await;
        if guard.client.is_none() {
          return Err(ControlClientError::Protocol(
            CLIENT_NOT_CONNECTED_MESSAGE.into(),
          ));
        }
        drop(guard);
      } else {
        match self.ensure_connection().await {
          Ok(()) => {}
          Err(err) => {
            if attempt == max_attempts {
              return Err(err);
            }
            let delay = self.config.retry.delay_for_attempt(attempt + 1);
            if delay.as_millis() > 0 {
              sleep(delay).await;
            }
            continue;
          }
        }
      }

      let mut guard = self.state.lock().await;
      let client = match guard.client.as_mut() {
        Some(client) => client,
        None => {
          if require_connected {
            return Err(ControlClientError::Protocol(
              CLIENT_NOT_CONNECTED_MESSAGE.into(),
            ));
          } else {
            drop(guard);
            continue;
          }
        }
      };

      match action(client).await {
        Ok(result) => {
          drop(guard);
          return Ok(result);
        }
        Err(err) => {
          if !self.is_retryable_error(&err) || attempt == max_attempts {
            drop(guard);
            return Err(err);
          }
          guard.client.take();
          require_connected = false;
          drop(guard);
          let delay = self.config.retry.delay_for_attempt(attempt + 1);
          if delay.as_millis() > 0 {
            sleep(delay).await;
          }
        }
      }
    }

    Err(ControlClientError::Protocol(
      "retry attempts exhausted".into(),
    ))
  }

  async fn ensure_connection(&self) -> ControlResult<()> {
    let params = self
      .config
      .handshake_params()
      .ok_or_else(|| ControlClientError::Protocol(MISSING_TOKEN_MESSAGE.into()))?;
    let mut guard = self.state.lock().await;
    if guard.client.is_some() {
      return Ok(());
    }
    let client = ControlClient::connect_with_tenant(
      &params.endpoint,
      params.token.as_str(),
      params.tenant_id.as_deref(),
    )
    .await?;
    guard.client = Some(client);
    Ok(())
  }

  fn is_retryable_error(&self, err: &ControlClientError) -> bool {
    matches!(
      err,
      ControlClientError::Io(_) | ControlClientError::Capnp(_)
    )
  }
}

#[napi]
pub fn create_client(options: Option<ClientOptions>) -> DbxClient {
  DbxClient::new(options)
}

fn ok_response_value() -> JsonValue {
  JsonValue::String("Ok".into())
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
  use std::sync::Mutex;
  use std::time::Duration;

  static ENV_LOCK: Mutex<()> = Mutex::new(());

  fn with_clean_verbose_env<T, F: FnOnce() -> T>(f: F) -> T {
    let _guard = ENV_LOCK.lock().expect("env lock poisoned");
    let previous = std::env::var("EVENTDBX_VERBOSE").ok();
    std::env::remove_var("EVENTDBX_VERBOSE");
    let result = f();
    match previous {
      Some(value) => std::env::set_var("EVENTDBX_VERBOSE", value),
      None => std::env::remove_var("EVENTDBX_VERBOSE"),
    }
    result
  }

  #[test]
  fn client_options_respects_verbose_true() {
    with_clean_verbose_env(|| {
      let options = ClientOptions {
        ip: None,
        port: None,
        token: None,
        tenant_id: None,
        verbose: Some(true),
        retry: None,
      };
      let config: ClientConfig = options.into();
      assert!(config.verbose, "expected verbose flag to propagate");
    });
  }

  #[test]
  fn client_options_respects_verbose_false() {
    with_clean_verbose_env(|| {
      std::env::set_var("EVENTDBX_VERBOSE", "1");
      let options = ClientOptions {
        ip: None,
        port: None,
        token: None,
        tenant_id: None,
        verbose: Some(false),
        retry: None,
      };
      let config: ClientConfig = options.into();
      assert!(
        !config.verbose,
        "explicit false should override environment default"
      );
    });
  }

  #[test]
  fn retry_options_override_defaults() {
    let options = ClientOptions {
      ip: None,
      port: None,
      token: None,
      tenant_id: None,
      verbose: None,
      retry: Some(RetryOptions {
        attempts: Some(3),
        initial_delay_ms: Some(200),
        max_delay_ms: Some(100),
      }),
    };
    let config: ClientConfig = options.into();
    assert_eq!(config.retry.attempts(), 3);
    assert_eq!(
      config.retry.delay_for_attempt(2),
      Duration::from_millis(200),
      "max delay should clamp to initial delay when smaller"
    );
    assert_eq!(
      config.retry.delay_for_attempt(4),
      Duration::from_millis(200),
    );
  }

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
