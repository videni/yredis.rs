use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;
use yrs::{updates::decoder::Decode, Doc as YDoc, ReadTxn, StateVector, Transact, Update};
use anyhow::Result;
use async_trait::async_trait;
use crate::storage::Storage;

use super::Reference;

pub struct MemoryStorageOpts;

pub fn create_memory_storage(_opts: Option<MemoryStorageOpts>) -> MemoryStorage {
    MemoryStorage::new()
}

pub struct MemoryStorage {
    // room -> docid -> referenceid -> doc_data
    docs: Mutex<HashMap<String, HashMap<String, HashMap<String, Vec<u8>>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            docs: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn persist_doc(&self, room: &str, docname: &str, ydoc: &YDoc) -> Result<()> {
        let mut docs = self.docs.lock().unwrap();
        let room_docs = docs
            .entry(room.to_string())
            .or_insert_with(HashMap::new);
        let doc_refs = room_docs
            .entry(docname.to_string())
            .or_insert_with(HashMap::new);
        
        let reference = Uuid::new_v4().to_string();
        let update = ydoc.transact_mut().encode_state_as_update_v2(&StateVector::default());

        doc_refs.insert(reference, update);
        
        Ok(())
    }

    async fn retrieve_doc(&self, room: &str, docname: &str) -> Result<Option<crate::storage::DocState>> {
        let docs = self.docs.lock().unwrap();
        
        if let Some(room_docs) = docs.get(room) {
            if let Some(doc_refs) = room_docs.get(docname) {
                if doc_refs.is_empty() {
                    return Ok(None);
                }
                
                let updates: Vec<Vec<u8>> = doc_refs.values().cloned().collect();
                let references: Vec<String> = doc_refs.keys().cloned().collect();
                
                let merged = Update::merge_updates(updates.iter().map(|u|{
                    Update::decode_v2( u.as_slice()).unwrap()
                }).collect::<Vec<_>>());
                
                let doc = YDoc::new();
                {
                    let mut txn = doc.transact_mut();
                    txn.apply_update(merged);
                }

                return Ok(Some(crate::storage::DocState {
                    doc,
                    references: references.into_iter().map(|r|r.into()).collect::<Vec<_>>(),
                }));
            }
        }
        
        Ok(None)
    }

    async fn delete_references(&self, room: &str, docname: &str, store_references: Vec<Reference>) -> Result<()> {
        let mut docs = self.docs.lock().unwrap();
        
        if let Some(room_docs) = docs.get_mut(room) {
            if let Some(doc_refs) = room_docs.get_mut(docname) {
                for reference in store_references {
                    doc_refs.remove(reference.downcast_ref::<String>().as_str());
                }
            }
        }
        
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_storage() {
        let storage = create_memory_storage(None);
        let ydoc = YDoc::new();
        
        // Test persist and retrieve Test persist and retrieve using tokio test runtime
        storage.persist_doc("room1", "doc1", &ydoc).await.unwrap();
        let doc_state = storage.retrieve_doc("room1", "doc1").await.unwrap();
        assert!(doc_state.is_some());
        
        // Test state vector
        let state_vector = storage.retrieve_state_vector("room1", "doc1").await.unwrap();
        assert!(state_vector.is_empty());
        
        // Test delete references
        if let Some(state) = doc_state {
            storage.delete_references("room1", "doc1", state.references).await.unwrap();
            let empty_doc = storage.retrieve_doc("room1", "doc1").await.unwrap();
            assert!(empty_doc.is_none());
        }
    }
}