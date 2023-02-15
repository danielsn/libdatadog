// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0. This product includes software
// developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present
// Datadog, Inc.

#![deny(clippy::all)]

pub mod pb {
    include!("../../trace-normalization/src/pb/pb.rs");
}

pub mod obfuscator;