// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use ddcommon::tag::Tag;
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct Serie {
    pub namespace: MetricNamespace,
    pub metric: String,
    pub points: Vec<(u64, f64)>,
    pub tags: Vec<Tag>,
    pub common: bool,
    #[serde(rename = "type")]
    pub _type: MetricType,
}

#[derive(Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum MetricNamespace {
    Trace,
    Profiling,
    Appsec,
}

#[derive(Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    #[serde(rename = "gauge")]
    Gauge,
    #[serde(rename = "count")]
    Count,
}
