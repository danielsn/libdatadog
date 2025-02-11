// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

pub use cc_utils::cc;

fn main() {
    let mut builder = cc_utils::ImprovedBuild::new();
    builder
        .file("src/trampoline.c")
        .warnings(true)
        .link_dynamically("dl")
        .warnings_into_errors(true)
        .flag("-std=c99")
        .emit_rerun_if_env_changed(true);

    if !cfg!(target_os = "windows") {
        builder.link_dynamically("dl");
        if cfg!(target_os = "linux") {
            builder.flag("-Wl,--no-as-needed");
        }
        builder.link_dynamically("m"); // rust code generally requires libm. Just link against it.
    }

    builder.try_compile_executable("trampoline.bin").unwrap();

    if !cfg!(target_os = "windows") {
        cc_utils::ImprovedBuild::new()
            .file("src/ld_preload_trampoline.c")
            .link_dynamically("dl")
            .warnings(true)
            .warnings_into_errors(true)
            .emit_rerun_if_env_changed(true)
            .try_compile_shared_lib("ld_preload_trampoline.shared_lib")
            .unwrap();
    }
}
