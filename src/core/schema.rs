//! Loads and validates runtime Protobuf descriptors.

// Rust guideline compliant 2026-02-21

use crate::{core::errors::DistillError, domain::MessageSchemaDefinition};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost_reflect::{DescriptorPool, MessageDescriptor};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct SchemaRegistry {
    schemas: Arc<RwLock<HashMap<String, RegisteredSchema>>>,
}

/// Caps descriptor memory so schema registration cannot exhaust the process budget.
const MAX_SCHEMA_CACHE_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
struct RegisteredSchema {
    descriptor: MessageDescriptor,
    encoded_size: usize,
}

impl SchemaRegistry {
    pub async fn remove(&self, channel_id: &str) {
        self.schemas.write().await.remove(channel_id);
    }

    pub async fn register(
        &self,
        channel_id: &str,
        definition: MessageSchemaDefinition,
    ) -> Result<(), DistillError> {
        if definition.message_type.is_empty() || definition.version.is_empty() {
            return Err(DistillError::InvalidConfiguration(
                "message_type and version are required".to_string(),
            ));
        }
        let bytes = STANDARD
            .decode(&definition.descriptor_set_base64)
            .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
        let pool = DescriptorPool::decode(bytes.as_slice())
            .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
        let descriptor = pool
            .get_message_by_name(&definition.message_type)
            .ok_or_else(|| {
                DistillError::InvalidConfiguration(format!(
                    "message descriptor {} was not found",
                    definition.message_type
                ))
            })?;
        let key = channel_id.to_string();
        let mut schemas = self.schemas.write().await;
        schemas.insert(
            key,
            RegisteredSchema {
                descriptor,
                encoded_size: bytes.len(),
            },
        );
        while schemas
            .values()
            .map(|schema| schema.encoded_size)
            .sum::<usize>()
            > MAX_SCHEMA_CACHE_BYTES
        {
            let Some(key) = schemas.keys().next().cloned() else {
                break;
            };
            schemas.remove(&key);
        }
        Ok(())
    }

    pub async fn validate_json(
        &self,
        channel_id: &str,
        payload: &serde_json::Value,
        payload_bytes: Option<&[u8]>,
    ) -> Result<(), DistillError> {
        let schemas = self.schemas.read().await;
        let Some(schema) = schemas.get(channel_id) else {
            return Ok(());
        };
        if let Some(bytes) = payload_bytes {
            prost_reflect::DynamicMessage::decode(schema.descriptor.clone(), bytes)
                .map_err(|error| DistillError::SchemaValidation(error.to_string()))?;
            return Ok(());
        }
        let object = payload.as_object().ok_or_else(|| {
            DistillError::SchemaValidation("payload must be a JSON object".to_string())
        })?;
        for field in object.keys() {
            if schema.descriptor.get_field_by_name(field).is_none() {
                return Err(DistillError::SchemaValidation(format!(
                    "unknown field in schema payload: {field}"
                )));
            }
        }
        Ok(())
    }
}
