use async_trait::async_trait;
use sea_orm::*;
use sea_orm::entity::prelude::*;

use yrs::updates::decoder::Decode;
use yrs::{updates::encoder::Encode, Doc, ReadTxn, StateVector, Transact, Update};
use anyhow::Result;

use super::{Reference, Storage};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "yredis_docs_v1")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub room: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub doc: String,
    #[sea_orm(primary_key, auto_increment = true)]
    pub r: i32,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub update: Vec<u8>,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub sv: Vec<u8>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

pub struct PostgresStorage {
    conn: DatabaseConnection,
}

impl PostgresStorage {
    pub async fn new(database_url: &str) -> Result<Self> {
        let conn = Database::connect(database_url).await?;
        
        // Create table if not exists
        let stmt = sea_orm::Statement::from_string(
            conn.get_database_backend(),
            r#"CREATE TABLE IF NOT EXISTS yredis_docs_v1 (
                room TEXT NOT NULL,
                doc TEXT NOT NULL,
                r SERIAL,
                update BYTEA NOT NULL,
                sv BYTEA NOT NULL,
                PRIMARY KEY (room, doc, r)
            );"#.to_owned()
        );
        conn.execute(stmt).await?;

        Ok(Self { conn })
    }
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn persist_doc(&self, room: &str, docname: &str, ydoc: &Doc) -> Result<()> {
        let update = ydoc.transact_mut().encode_state_as_update_v2(&StateVector::default());
        let sv = ydoc.transact_mut().state_vector().encode_v2();

        let model = ActiveModel {
            room: Set(room.to_owned()),
            doc: Set(docname.to_owned()),
            update: Set(update),
            sv: Set(sv),
            ..Default::default()
        };

        model.insert(&self.conn).await?;
        Ok(())
    }

    async fn retrieve_doc(&self, room: &str, docname: &str) -> Result<Option<crate::storage::DocState>> {
        let docs = Entity::find()
            .filter(
                Condition::all()
                    .add(Column::Room.eq(room))
                    .add(Column::Doc.eq(docname))
            )
            .all(&self.conn)
            .await?;

        if docs.is_empty() {
            return Ok(None);
        }

        let updates = docs.iter()
            .map(|m| m.update.clone())
            .collect::<Vec<_>>();
        let references = docs.iter().map(|m| m.r.into()).collect::<Vec<_>>();

        let merged = Update::merge_updates(updates.iter().map(|u|{
            Update::decode_v2( u.as_slice()).unwrap()
        }).collect::<Vec<_>>());
        
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            txn.apply_update(merged);
        }


        return Ok(Some(crate::storage::DocState {
            doc,
            references,
        }));
    }

    async fn retrieve_state_vector(&self, room: &str, docname: &str) -> Result<Vec<u8>> {
        let doc = Entity::find()
            .filter(
                Condition::all()
                    .add(Column::Room.eq(room))
                    .add(Column::Doc.eq(docname))
            )
            .order_by_desc(Column::R)
            .one(&self.conn)
            .await?;

        Ok(doc.map(|m| m.sv).unwrap_or_default())
    }

    async fn delete_references(&self, room: &str, docname: &str, refs: Vec<Reference>) -> Result<()> {
        let _ = Entity::delete_many()
        .filter(
            Condition::all()
                .add(Column::Room.eq(room))
                .add(Column::Doc.eq(docname))
                .add(Column::R.is_in(refs.into_iter().map(|r|r.downcast_ref::<i32>())))
        )
        .exec(&self.conn)
        .await?;

        Ok(())
    }
}