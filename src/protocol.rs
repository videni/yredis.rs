use anyhow::{Result, anyhow};
use tracing::error;
use yrs::{
    encoding::{read::{Cursor, Read}, write::Write},
    sync::{Awareness, AwarenessUpdate},
    updates::{decoder::{Decode, DecoderV1}, encoder::{Encode, Encoder, EncoderV1}},
    Doc as YDoc,
    Transact, Update,
};

// Message types
pub const MSG_SYNC: u8 = 0;
pub const MSG_AWARENESS: u8 = 1;
pub const MSG_AUTH: u8 = 2;
pub const MSG_QUERY_AWARENESS: u8 = 3;

// Sync message subtypes
pub const MSG_SYNC_STEP1: u8 = 0;
pub const MSG_SYNC_STEP2: u8 = 1;
pub const MSG_SYNC_UPDATE: u8 = 2;

/// Merges multiple messages for easier consumption by the client.
///
/// This is useful when the server catches messages from a Redis stream.
/// Before sending messages to clients, we can merge updates and filter out older
/// awareness messages.
pub fn merge_messages(messages: &[Vec<u8>]) -> Result<Vec<u8>> {
    if messages.len() < 2 {
        return Ok(messages.iter().flat_map(|m|m.to_vec()).collect());
    }

    let ydoc = YDoc::new();
    let awareness = Awareness::new(ydoc);
    let mut updates = Vec::new();

    for message in messages {
        let mut decoder = DecoderV1::new(Cursor::new(message));
        
        match decoder.read_u8() {
            Ok(MSG_SYNC) => {
                match decoder.read_u8() {
                    Ok(MSG_SYNC_UPDATE) => {
                        if let Ok(update_data) = decoder.read_buf() {
                            updates.push(Update::decode_v1(update_data.to_vec().as_slice())?);
                        }
                    },
                    _ => continue, // Skip other sync message types
                }
            },
            Ok(MSG_AWARENESS) => {
                if let Ok(awareness_data) = decoder.read_buf() {
                    if let Ok(update) = AwarenessUpdate::decode_v1(&awareness_data) {
                        let _ = awareness.apply_update(update);
                    }
                }
            },
            _ => continue, // Skip unknown message types
        }
    }

    let mut encoder = EncoderV1::new();

    // Add merged updates if any
    if !updates.is_empty() {
        encoder.write_u8(MSG_SYNC);
        encoder.write_u8(MSG_SYNC_UPDATE);
        
        let merged_updates: Update = Update::merge_updates(updates);
        encoder.write_buf(merged_updates.encode_v1());
    }
    
    let states = awareness.iter().collect::<Vec<_>>();
    if !states.is_empty() {
        let client_ids: Vec<u64> = states.iter().map(|(id, _)| *id).collect();
        let awareness_update = awareness.update_with_clients(client_ids)?;

        encoder.write_u8(MSG_AWARENESS);
        encoder.write_buf(awareness_update.encode_v1());
    }
    
    Ok(encoder.to_vec())
}

/// Encodes a sync step 1 message with state vector
pub fn encode_sync_step1(sv: &[u8]) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    encoder.write_u8(MSG_SYNC);
    encoder.write_u8(MSG_SYNC_STEP1);
    encoder.write_buf(sv);
    
    encoder.to_vec()
}

/// Encodes a sync step 2 message with update data
pub fn encode_sync_step2(diff: &[u8]) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    encoder.write_u8(MSG_SYNC);
    encoder.write_u8(MSG_SYNC_STEP2);
    encoder.write_buf(diff);
    encoder.to_vec()
}

/// Encodes an awareness update message
pub fn encode_awareness_update(awareness: &Awareness, clients: Vec<u64>) -> Result<Vec<u8>> {
    let mut encoder = EncoderV1::new();
    encoder.write_u8(MSG_AWARENESS);
    let awareness_data = awareness.update_with_clients(clients)?;
    encoder.write_buf(&awareness_data.encode_v1());

    Ok(encoder.to_vec())
}

/// Encodes an awareness message for a disconnected user
pub fn encode_awareness_user_disconnected(client_id: u64, last_clock: u64) -> Result<Vec<u8>> {
    let mut encoder = EncoderV1::new();
    encoder.write_u8(MSG_AWARENESS);
    
    let mut sub_encoder = EncoderV1::new();
    sub_encoder.write_u64 (1u64); 
    sub_encoder.write_u64(client_id);
    sub_encoder.write_u64(last_clock + 1);
    sub_encoder.write_string("null"); // Null JSON state
    
    encoder.write_buf(&sub_encoder.to_vec());
    
    Ok(encoder.to_vec())
}