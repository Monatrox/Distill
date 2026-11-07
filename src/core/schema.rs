//! Loads and validates runtime Protobuf descriptors.

// Rust guideline compliant 2026-02-21

use crate::{core::errors::DistillError, domain::MessageSchemaDefinition};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost_reflect::{DescriptorPool, MessageDescriptor};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct SchemaRegistry {
    schemas: Arc<RwLock<HashMap<String, PreparedSchema>>>,
}

/// Caps descriptor memory so schema registration cannot exhaust the process budget.
const MAX_SCHEMA_CACHE_BYTES: usize = 100 * 1024 * 1024;

/// Caps a single decoded descriptor set.
///
/// One MiB is far above any realistic message schema while preventing a single
/// registration from allocating unbounded memory during base64 decoding.
const MAX_SCHEMA_DESCRIPTOR_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct PreparedSchema {
    descriptor: MessageDescriptor,
    encoded_size: usize,
}

impl SchemaRegistry {
    pub(crate) fn prepare(
        definition: &MessageSchemaDefinition,
    ) -> Result<PreparedSchema, DistillError> {
        if definition.message_type.is_empty() || definition.version.is_empty() {
            return Err(DistillError::InvalidConfiguration(
                "message_type and version are required".to_string(),
            ));
        }
        let bytes = STANDARD
            .decode(&definition.descriptor_set_base64)
            .map_err(|error| DistillError::InvalidConfiguration(error.to_string()))?;
        if bytes.len() > MAX_SCHEMA_DESCRIPTOR_BYTES {
            return Err(DistillError::InvalidConfiguration(format!(
                "schema descriptor exceeds the {MAX_SCHEMA_DESCRIPTOR_BYTES} byte limit"
            )));
        }
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
        Ok(PreparedSchema {
            descriptor,
            encoded_size: bytes.len(),
        })
    }

    /// Rejects schema growth rather than silently disabling existing validation.
    pub(crate) async fn check_capacity(
        &self,
        channel_id: &str,
        schema: Option<&PreparedSchema>,
    ) -> Result<(), DistillError> {
        let schemas = self.schemas.read().await;
        let bytes: usize = schemas
            .iter()
            .filter(|(key, _)| key.as_str() != channel_id)
            .map(|(_, schema)| schema.encoded_size)
            .sum();
        if bytes.saturating_add(schema.map_or(0, |schema| schema.encoded_size))
            > MAX_SCHEMA_CACHE_BYTES
        {
            return Err(DistillError::BufferFull("schema registry".to_string()));
        }
        Ok(())
    }

    pub(crate) async fn activate(&self, channel_id: &str, schema: Option<PreparedSchema>) {
        let mut schemas = self.schemas.write().await;
        if let Some(schema) = schema {
            schemas.insert(channel_id.to_string(), schema);
        } else {
            schemas.remove(channel_id);
        }
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
        prost_reflect::DynamicMessage::deserialize(schema.descriptor.clone(), payload).map_err(
            |_| {
                DistillError::SchemaValidation(
                    "payload does not match the registered schema".to_string(),
                )
            },
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };

    fn schema() -> MessageSchemaDefinition {
        let descriptor = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".to_string()),
                package: Some("test".to_string()),
                syntax: Some("proto3".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("Counter".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("value".to_string()),
                        number: Some(1),
                        label: Some(1),
                        r#type: Some(5),
                        ..FieldDescriptorProto::default()
                    }],
                    ..DescriptorProto::default()
                }],
                ..FileDescriptorProto::default()
            }],
        };
        MessageSchemaDefinition {
            message_type: "test.Counter".to_string(),
            version: "1".to_string(),
            descriptor_set_base64: STANDARD.encode(descriptor.encode_to_vec()),
        }
    }

    #[tokio::test]
    async fn schemas_validate_types_and_capacity_does_not_disable_validation() {
        let registry = SchemaRegistry::default();
        let mut prepared = SchemaRegistry::prepare(&schema()).unwrap();
        prepared.encoded_size = MAX_SCHEMA_CACHE_BYTES;
        registry
            .check_capacity("counter@1", Some(&prepared))
            .await
            .unwrap();
        registry.activate("counter@1", Some(prepared.clone())).await;
        assert!(
            registry
                .validate_json("counter@1", &serde_json::json!({"value": 7}), None)
                .await
                .is_ok()
        );
        assert!(
            registry
                .validate_json(
                    "counter@1",
                    &serde_json::json!({"value": "not-an-integer"}),
                    None
                )
                .await
                .is_err()
        );
        assert!(
            registry
                .check_capacity("counter@2", Some(&prepared))
                .await
                .is_err()
        );
        assert!(
            registry
                .validate_json(
                    "counter@1",
                    &serde_json::json!({"secret": "must-not-appear"}),
                    None
                )
                .await
                .is_err()
        );
        let error = registry
            .validate_json(
                "counter@1",
                &serde_json::json!({"value": "must-not-appear"}),
                None,
            )
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("must-not-appear"));
    }
}
