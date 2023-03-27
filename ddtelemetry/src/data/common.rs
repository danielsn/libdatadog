// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use serde::{Deserialize, Serialize};

use crate::data::*;

#[derive(Serialize, Deserialize, Debug)]
pub enum ApiVersion {
    #[serde(rename = "v1")]
    V1,
}

#[derive(Serialize, Debug)]
pub struct Telemetry<'a> {
    pub api_version: ApiVersion,
    pub tracer_time: u64,
    pub runtime_id: &'a str,
    pub seq_id: u64,
    pub application: &'a Application,
    pub host: &'a Host,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Application {
    pub service_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    pub language_name: String,
    pub language_version: String,
    pub tracer_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_patches: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Host {
    pub hostname: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kernel_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kernel_release: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kernel_version: Option<String>,
}
