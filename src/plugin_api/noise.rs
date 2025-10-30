use std::io::ErrorKind;

use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use sha2::{Digest, Sha256};
use snow::{params::NoiseParams, TransportState};

use super::ControlClientError;

const NOISE_PROTOCOL_NAME: &str = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const AEAD_TAG_LEN: usize = 16;
const HANDSHAKE_MESSAGE_MAX: usize = 1024;

fn derive_psk(token: &[u8]) -> [u8; 32] {
  let mut hasher = Sha256::new();
  hasher.update(token);
  let digest = hasher.finalize();
  let mut psk = [0u8; 32];
  psk.copy_from_slice(&digest);
  psk
}

pub async fn perform_client_handshake<R, W>(
  reader: &mut R,
  writer: &mut W,
  token: &[u8],
) -> Result<TransportState, ControlClientError>
where
  R: AsyncRead + Unpin,
  W: AsyncWrite + Unpin,
{
  let params: NoiseParams = NOISE_PROTOCOL_NAME
    .parse()
    .map_err(|err| ControlClientError::Protocol(format!("failed to parse Noise protocol: {err}")))?;
  let psk = derive_psk(token);
  let builder = snow::Builder::new(params).psk(0, &psk);
  let mut state = builder
    .build_initiator()
    .map_err(|err| ControlClientError::Protocol(format!("failed to build Noise initiator: {err}")))?;

  let mut buffer = vec![0u8; HANDSHAKE_MESSAGE_MAX];
  let len = state
    .write_message(&[], &mut buffer)
    .map_err(|err| ControlClientError::Protocol(format!("failed to write Noise handshake: {err}")))?;
  send_frame(writer, &buffer[..len]).await?;
  writer.flush().await?; // Io errors bubble up

  let frame = match read_frame(reader).await? {
    Some(frame) => frame,
    None => {
      return Err(ControlClientError::Protocol(
        "peer closed connection during Noise handshake".into(),
      ))
    }
  };
  state
    .read_message(&frame, &mut [])
    .map_err(|err| ControlClientError::Protocol(format!("failed to read Noise handshake: {err}")))?;
  state
    .into_transport_mode()
    .map_err(|err| ControlClientError::Protocol(format!("failed to initialize Noise transport: {err}")))
}

pub async fn write_encrypted_frame<W>(
  writer: &mut W,
  state: &mut TransportState,
  plaintext: &[u8],
) -> Result<(), ControlClientError>
where
  W: AsyncWrite + Unpin,
{
  if plaintext.len() > MAX_FRAME_LEN {
    return Err(ControlClientError::Protocol(format!(
      "plaintext exceeds maximum frame size ({} bytes)",
      MAX_FRAME_LEN
    )));
  }

  let mut buffer = vec![0u8; plaintext.len() + AEAD_TAG_LEN];
  let len = state
    .write_message(plaintext, &mut buffer)
    .map_err(|err| ControlClientError::Protocol(format!("failed to encrypt Noise frame: {err}")))?;
  send_frame(writer, &buffer[..len]).await?;
  writer.flush().await?;
  Ok(())
}

pub async fn read_encrypted_frame<R>(
  reader: &mut R,
  state: &mut TransportState,
) -> Result<Option<Vec<u8>>, ControlClientError>
where
  R: AsyncRead + Unpin,
{
  let frame = match read_frame(reader).await? {
    Some(frame) => frame,
    None => return Ok(None),
  };
  if frame.len() > MAX_FRAME_LEN + AEAD_TAG_LEN {
    return Err(ControlClientError::Protocol(format!(
      "encrypted frame exceeds maximum size ({} bytes)",
      MAX_FRAME_LEN + AEAD_TAG_LEN
    )));
  }
  let mut buffer = vec![0u8; frame.len()];
  let len = state
    .read_message(&frame, &mut buffer)
    .map_err(|err| ControlClientError::Protocol(format!("failed to decrypt Noise frame: {err}")))?;
  buffer.truncate(len);
  Ok(Some(buffer))
}

async fn send_frame<W>(writer: &mut W, payload: &[u8]) -> Result<(), ControlClientError>
where
  W: AsyncWrite + Unpin,
{
  if payload.len() > u32::MAX as usize {
    return Err(ControlClientError::Protocol(
      "frame payload exceeds u32 length".into(),
    ));
  }
  let mut header = [0u8; 4];
  header.copy_from_slice(&(payload.len() as u32).to_be_bytes());
  writer.write_all(&header).await?;
  writer.write_all(payload).await?;
  Ok(())
}

async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>, ControlClientError>
where
  R: AsyncRead + Unpin,
{
  let mut header = [0u8; 4];
  match reader.read_exact(&mut header).await {
    Ok(()) => {}
    Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
    Err(err) => return Err(ControlClientError::Io(err)),
  }
  let len = u32::from_be_bytes(header) as usize;
  if len > MAX_FRAME_LEN + AEAD_TAG_LEN {
    return Err(ControlClientError::Protocol(format!(
      "frame length {} exceeds maximum {}",
      len,
      MAX_FRAME_LEN + AEAD_TAG_LEN
    )));
  }
  let mut payload = vec![0u8; len];
  if len > 0 {
    match reader.read_exact(&mut payload).await {
      Ok(()) => {}
      Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
      Err(err) => return Err(ControlClientError::Io(err)),
    }
  }
  Ok(Some(payload))
}
