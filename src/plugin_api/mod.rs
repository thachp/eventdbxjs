//! Lightweight control-socket bindings for EventDBX.
//!
//! This module exposes serde-friendly structures plus a thin asynchronous
//! client that speaks the Cap'n Proto control protocol on port 6363.

pub mod control_capnp {
  include!(concat!(env!("OUT_DIR"), "/control_capnp.rs"));
}

use capnp::message::{Builder, ReaderOptions};
use capnp::serialize::write_message_to_words;
use capnp_futures::serialize::read_message;
use chrono::{DateTime, Utc};
use futures::io::AsyncWriteExt;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

/// Aggregate snapshot returned by the control socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AggregateStateView {
  pub aggregate_type: String,
  pub aggregate_id: String,
  pub version: u64,
  pub state: BTreeMap<String, String>,
  pub merkle_root: String,
  pub archived: bool,
}

/// Actor identity associated with an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorClaimsView {
  pub group: String,
  pub user: String,
}

/// Event metadata as returned by the control socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventMetadataView {
  #[serde(alias = "event_id")]
  pub event_id: String,
  #[serde(
    serialize_with = "serde_helpers::serialize_rfc3339",
    deserialize_with = "serde_helpers::deserialize_rfc3339"
  )]
  #[serde(alias = "created_at")]
  pub created_at: DateTime<Utc>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(alias = "issued_by")]
  pub issued_by: Option<ActorClaimsView>,
  #[serde(skip_serializing_if = "Option::is_none")]
  #[serde(alias = "note")]
  pub note: Option<String>,
}

/// Event record returned by the control socket when listing or appending events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredEventRecord {
  #[serde(alias = "aggregate_type")]
  pub aggregate_type: String,
  #[serde(alias = "aggregate_id")]
  pub aggregate_id: String,
  #[serde(alias = "event_type")]
  pub event_type: String,
  #[serde(alias = "version")]
  pub version: u64,
  #[serde(alias = "payload")]
  pub payload: JsonValue,
  #[serde(alias = "metadata")]
  pub metadata: EventMetadataView,
  #[serde(alias = "hash")]
  pub hash: String,
  #[serde(alias = "merkle_root")]
  pub merkle_root: String,
}

/// Request payload for appending an event through the control socket.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppendEventRequest {
  pub token: String,
  pub aggregate_type: String,
  pub aggregate_id: String,
  pub event_type: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub payload: Option<JsonValue>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub metadata: Option<JsonValue>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub note: Option<String>,
  #[serde(default)]
  pub require_existing: bool,
}

/// Request payload for creating an aggregate, optionally emitting an initial event.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateAggregateRequest {
  pub token: String,
  pub aggregate_type: String,
  pub aggregate_id: String,
  pub event_type: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub payload: Option<JsonValue>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub metadata: Option<JsonValue>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub note: Option<String>,
}

/// Request payload for issuing a JSON Patch against an aggregate.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PatchEventRequest {
  pub token: String,
  pub aggregate_type: String,
  pub aggregate_id: String,
  pub event_type: String,
  pub patch: JsonValue,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub metadata: Option<JsonValue>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub note: Option<String>,
}

mod serde_helpers {
  use chrono::{DateTime, Utc};
  use serde::{self, Deserialize, Deserializer, Serializer};

  pub fn serialize_rfc3339<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    serializer.serialize_str(&value.to_rfc3339())
  }

  pub fn deserialize_rfc3339<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
  where
    D: Deserializer<'de>,
  {
    let raw = String::deserialize(deserializer)?;
    chrono::DateTime::parse_from_rfc3339(&raw)
      .map(|dt| dt.with_timezone(&Utc))
      .map_err(serde::de::Error::custom)
  }
}

/// Errors produced by the control-socket client.
#[derive(Debug, Error)]
pub enum ControlClientError {
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("capnp error: {0}")]
  Capnp(#[from] capnp::Error),
  #[error("server error ({code}): {message}")]
  Server { code: String, message: String },
  #[error("protocol error: {0}")]
  Protocol(String),
  #[error("json error: {0}")]
  Json(#[from] serde_json::Error),
}

pub type ControlResult<T> = Result<T, ControlClientError>;

/// Minimal client for the control socket exposed on port 6363.
pub struct ControlClient {
  stream: Compat<TcpStream>,
  next_id: u64,
}

impl ControlClient {
  /// Connect to the control socket at `addr` (e.g. `"127.0.0.1:6363"`).
  pub async fn connect(addr: &str) -> ControlResult<Self> {
    let stream = TcpStream::connect(addr).await?;
    Ok(Self {
      stream: stream.compat(),
      next_id: 1,
    })
  }

  /// Retrieve a sanitized list of aggregates.
  pub async fn list_aggregates(
    &mut self,
    skip: usize,
    take: Option<usize>,
    include_archived: bool,
    archived_only: bool,
  ) -> ControlResult<Vec<AggregateStateView>> {
    let skip_u64 = u64::try_from(skip)
      .map_err(|_| ControlClientError::Protocol("skip exceeds u64 range".into()))?;
    let take_u64 = match take {
      Some(value) => Some(
        u64::try_from(value)
          .map_err(|_| ControlClientError::Protocol("take exceeds u64 range".into()))?,
      ),
      None => None,
    };

    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut request = message.init_root::<control_capnp::control_request::Builder>();
      request.set_id(request_id);
      let payload = request.reborrow().init_payload();
      let mut body = payload.init_list_aggregates();
      body.set_skip(skip_u64);
      if let Some(take) = take_u64 {
        body.set_has_take(true);
        body.set_take(take);
      } else {
        body.set_has_take(false);
        body.set_take(0);
      }
      body.set_include_archived(include_archived);
      body.set_archived_only(archived_only);
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for listAggregates".into())
        })? {
          control_capnp::control_response::payload::ListAggregates(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            let json = read_text_field(resp.get_aggregates_json(), "aggregates_json")?;
            let aggregates = serde_json::from_str::<Vec<AggregateStateView>>(&json)?;
            Ok(aggregates)
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to listAggregates".into(),
          )),
        }
      })
      .await
  }

  /// Fetch a single aggregate if it exists.
  pub async fn get_aggregate(
    &mut self,
    aggregate_type: &str,
    aggregate_id: &str,
  ) -> ControlResult<Option<AggregateStateView>> {
    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut request = message.init_root::<control_capnp::control_request::Builder>();
      request.set_id(request_id);
      let payload = request.reborrow().init_payload();
      let mut body = payload.init_get_aggregate();
      body.set_aggregate_type(aggregate_type.into());
      body.set_aggregate_id(aggregate_id.into());
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for getAggregate".into())
        })? {
          control_capnp::control_response::payload::GetAggregate(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            if resp.get_found() {
              let json = read_text_field(resp.get_aggregate_json(), "aggregate_json")?;
              let aggregate = serde_json::from_str::<AggregateStateView>(&json)?;
              Ok(Some(aggregate))
            } else {
              Ok(None)
            }
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to getAggregate".into(),
          )),
        }
      })
      .await
  }

  /// List events for a given aggregate.
  pub async fn list_events(
    &mut self,
    aggregate_type: &str,
    aggregate_id: &str,
    skip: usize,
    take: Option<usize>,
  ) -> ControlResult<Vec<StoredEventRecord>> {
    let skip_u64 = u64::try_from(skip)
      .map_err(|_| ControlClientError::Protocol("skip exceeds u64 range".into()))?;
    let take_u64 = match take {
      Some(value) => Some(
        u64::try_from(value)
          .map_err(|_| ControlClientError::Protocol("take exceeds u64 range".into()))?,
      ),
      None => None,
    };

    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut request = message.init_root::<control_capnp::control_request::Builder>();
      request.set_id(request_id);
      let payload = request.reborrow().init_payload();
      let mut body = payload.init_list_events();
      body.set_aggregate_type(aggregate_type.into());
      body.set_aggregate_id(aggregate_id.into());
      body.set_skip(skip_u64);
      if let Some(take) = take_u64 {
        body.set_has_take(true);
        body.set_take(take);
      } else {
        body.set_has_take(false);
        body.set_take(0);
      }
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for listEvents".into())
        })? {
          control_capnp::control_response::payload::ListEvents(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            let json = read_text_field(resp.get_events_json(), "events_json")?;
            let events = serde_json::from_str::<Vec<StoredEventRecord>>(&json)?;
            Ok(events)
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to listEvents".into(),
          )),
        }
      })
      .await
  }

  /// Select specific fields from an aggregate snapshot.
  pub async fn select_aggregate(
    &mut self,
    aggregate_type: &str,
    aggregate_id: &str,
    fields: &[String],
  ) -> ControlResult<Option<JsonValue>> {
    let field_count = u32::try_from(fields.len())
      .map_err(|_| ControlClientError::Protocol("fields length exceeds u32 range".into()))?;

    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut request = message.init_root::<control_capnp::control_request::Builder>();
      request.set_id(request_id);
      let payload = request.reborrow().init_payload();
      let mut body = payload.init_select_aggregate();
      body.set_aggregate_type(aggregate_type.into());
      body.set_aggregate_id(aggregate_id.into());
      let mut field_list = body.reborrow().init_fields(field_count);
      for (idx, field) in fields.iter().enumerate() {
        field_list.set(idx as u32, field.as_str().into());
      }
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for selectAggregate".into())
        })? {
          control_capnp::control_response::payload::SelectAggregate(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            if resp.get_found() {
              let json = read_text_field(resp.get_selection_json(), "selection_json")?;
              let value = serde_json::from_str::<JsonValue>(&json)?;
              Ok(Some(value))
            } else {
              Ok(None)
            }
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to selectAggregate".into(),
          )),
        }
      })
      .await
  }

  /// Create an aggregate, optionally recording the first event.
  pub async fn create_aggregate(
    &mut self,
    request: CreateAggregateRequest,
  ) -> ControlResult<AggregateStateView> {
    let payload_json = request
      .payload
      .as_ref()
      .map(serde_json::to_string)
      .transpose()?
      .unwrap_or_else(|| "null".to_string());
    let metadata_json = request
      .metadata
      .as_ref()
      .map(serde_json::to_string)
      .transpose()?;

    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut cap_request = message.init_root::<control_capnp::control_request::Builder>();
      cap_request.set_id(request_id);
      let payload = cap_request.reborrow().init_payload();
      let mut body = payload.init_create_aggregate();
      body.set_token(request.token.as_str().into());
      body.set_aggregate_type(request.aggregate_type.as_str().into());
      body.set_aggregate_id(request.aggregate_id.as_str().into());
      body.set_event_type(request.event_type.as_str().into());
      body.set_payload_json(payload_json.as_str().into());
      if let Some(metadata) = metadata_json.as_ref() {
        body.set_has_metadata(true);
        body.set_metadata_json(metadata.as_str().into());
      } else {
        body.set_has_metadata(false);
        body.set_metadata_json("".into());
      }
      if let Some(note) = request.note.as_ref() {
        body.set_has_note(true);
        body.set_note(note.as_str().into());
      } else {
        body.set_has_note(false);
        body.set_note("".into());
      }
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for createAggregate".into())
        })? {
          control_capnp::control_response::payload::CreateAggregate(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            let json = read_text_field(resp.get_aggregate_json(), "aggregate_json")?;
            let aggregate = serde_json::from_str::<AggregateStateView>(&json)?;
            Ok(aggregate)
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to createAggregate".into(),
          )),
        }
      })
      .await
  }

  /// Append an event to an aggregate.
  pub async fn append_event(
    &mut self,
    request: AppendEventRequest,
  ) -> ControlResult<StoredEventRecord> {
    let payload_json = request
      .payload
      .as_ref()
      .map(serde_json::to_string)
      .transpose()?
      .unwrap_or_else(|| "null".to_string());
    let metadata_json = request
      .metadata
      .as_ref()
      .map(serde_json::to_string)
      .transpose()?;

    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut cap_request = message.init_root::<control_capnp::control_request::Builder>();
      cap_request.set_id(request_id);
      let payload = cap_request.reborrow().init_payload();
      let mut body = payload.init_append_event();
      body.set_token(request.token.as_str().into());
      body.set_aggregate_type(request.aggregate_type.as_str().into());
      body.set_aggregate_id(request.aggregate_id.as_str().into());
      body.set_event_type(request.event_type.as_str().into());
      body.set_payload_json(payload_json.as_str().into());
      if let Some(metadata) = metadata_json.as_ref() {
        body.set_has_metadata(true);
        body.set_metadata_json(metadata.as_str().into());
      } else {
        body.set_has_metadata(false);
        body.set_metadata_json("".into());
      }
      if let Some(note) = request.note.as_ref() {
        body.set_has_note(true);
        body.set_note(note.as_str().into());
      } else {
        body.set_has_note(false);
        body.set_note("".into());
      }
      body.set_require_existing(request.require_existing);
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for appendEvent".into())
        })? {
          control_capnp::control_response::payload::AppendEvent(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            let json = read_text_field(resp.get_event_json(), "event_json")?;
            let record = serde_json::from_str::<StoredEventRecord>(&json)?;
            Ok(record)
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to appendEvent".into(),
          )),
        }
      })
      .await
  }

  /// Apply a JSON Patch to an aggregate, recording a new event.
  pub async fn patch_event(
    &mut self,
    request: PatchEventRequest,
  ) -> ControlResult<StoredEventRecord> {
    if !request.patch.is_array() {
      return Err(ControlClientError::Protocol(
        "patch payload must be a JSON array".into(),
      ));
    }

    let patch_json = serde_json::to_string(&request.patch)?;
    let metadata_json = request
      .metadata
      .as_ref()
      .map(serde_json::to_string)
      .transpose()?;

    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut cap_request = message.init_root::<control_capnp::control_request::Builder>();
      cap_request.set_id(request_id);
      let payload = cap_request.reborrow().init_payload();
      let mut body = payload.init_patch_event();
      body.set_token(request.token.as_str().into());
      body.set_aggregate_type(request.aggregate_type.as_str().into());
      body.set_aggregate_id(request.aggregate_id.as_str().into());
      body.set_event_type(request.event_type.as_str().into());
      body.set_patch_json(patch_json.as_str().into());
      if let Some(metadata) = metadata_json.as_ref() {
        body.set_has_metadata(true);
        body.set_metadata_json(metadata.as_str().into());
      } else {
        body.set_has_metadata(false);
        body.set_metadata_json("".into());
      }
      if let Some(note) = request.note.as_ref() {
        body.set_has_note(true);
        body.set_note(note.as_str().into());
      } else {
        body.set_has_note(false);
        body.set_note("".into());
      }
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for patchEvent".into())
        })? {
          control_capnp::control_response::payload::AppendEvent(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            let json = read_text_field(resp.get_event_json(), "event_json")?;
            let record = serde_json::from_str::<StoredEventRecord>(&json)?;
            Ok(record)
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to patchEvent".into(),
          )),
        }
      })
      .await
  }

  /// Verify an aggregate's merkle root.
  pub async fn verify_aggregate(
    &mut self,
    aggregate_type: &str,
    aggregate_id: &str,
  ) -> ControlResult<String> {
    let request_id = self.next_request_id();
    let mut message = Builder::new_default();
    {
      let mut request = message.init_root::<control_capnp::control_request::Builder>();
      request.set_id(request_id);
      let payload = request.reborrow().init_payload();
      let mut body = payload.init_verify_aggregate();
      body.set_aggregate_type(aggregate_type.into());
      body.set_aggregate_id(aggregate_id.into());
    }

    self
      .send_and_parse(message, request_id, |response| {
        match response.get_payload().which().map_err(|_| {
          ControlClientError::Protocol("unexpected response payload for verifyAggregate".into())
        })? {
          control_capnp::control_response::payload::VerifyAggregate(resp) => {
            let resp = resp.map_err(ControlClientError::Capnp)?;
            let merkle = read_text_field(resp.get_merkle_root(), "merkle_root")?;
            Ok(merkle)
          }
          control_capnp::control_response::payload::Error(err) => {
            let err = err.map_err(ControlClientError::Capnp)?;
            Err(server_error(err))
          }
          _ => Err(ControlClientError::Protocol(
            "unexpected response to verifyAggregate".into(),
          )),
        }
      })
      .await
  }

  fn next_request_id(&mut self) -> u64 {
    let id = self.next_id;
    self.next_id = self.next_id.wrapping_add(1);
    id
  }

  async fn send_and_parse<F, T>(
    &mut self,
    message: Builder<capnp::message::HeapAllocator>,
    request_id: u64,
    parser: F,
  ) -> ControlResult<T>
  where
    F: FnOnce(control_capnp::control_response::Reader<'_>) -> ControlResult<T>,
  {
    let bytes = write_message_to_words(&message);
    self.stream.write_all(&bytes).await?;

    let response_message = read_message(&mut self.stream, ReaderOptions::new()).await?;
    let response = response_message
      .get_root::<control_capnp::control_response::Reader>()
      .map_err(ControlClientError::Capnp)?;

    if response.get_id() != request_id {
      return Err(ControlClientError::Protocol(format!(
        "mismatched response id (expected {}, got {})",
        request_id,
        response.get_id()
      )));
    }

    parser(response)
  }
}

fn read_text_field(
  field: capnp::Result<capnp::text::Reader<'_>>,
  label: &str,
) -> ControlResult<String> {
  let reader = field.map_err(ControlClientError::Capnp)?;
  reader
    .to_string()
    .map_err(|err| ControlClientError::Protocol(format!("invalid UTF-8 in {label}: {err}")))
}

fn server_error(reader: control_capnp::control_error::Reader<'_>) -> ControlClientError {
  let code =
    read_text_field(reader.get_code(), "error code").unwrap_or_else(|_| "unknown".to_string());
  let message = read_text_field(reader.get_message(), "error message")
    .unwrap_or_else(|_| "unknown".to_string());
  ControlClientError::Server { code, message }
}
