pub mod memory;

use anyhow::Result;
use async_trait::async_trait;
use yrs::{updates::encoder::Encode, Transact, Doc as YDoc, ReadTxn};
use std::vec;

#[derive(Debug)]
pub struct DocState {
    pub doc: YDoc,
    pub references: Vec<String>,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn persist_doc(&self, _room: &str, _docname: &str, _ydoc: &YDoc) -> Result<()> {
        unimplemented!("persist_doc not implemented")
    }

    async fn retrieve_doc(&self, _room: &str, _docname: &str) -> Result<Option<DocState>> {
        unimplemented!("retrieve_doc not implemented")
    }

    async fn retrieve_state_vector(&self, room: &str, docname: &str) -> Result<Vec<u8>> {
        let doc_state = self.retrieve_doc(room, docname).await?;
        
        if let Some(doc_state) = doc_state {
            let txn = doc_state.doc.transact_mut();
            let update = txn.state_vector().encode_v2();
            Ok(update)
        } else {
            Ok(vec![])
        }
    }

    async fn delete_references(
        &self, 
        _room: &str, 
        _docname: &str, 
        _store_references: Vec<String>
    ) -> Result<()> {
        unimplemented!("delete_references not implemented")
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
    }
}