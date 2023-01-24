// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present Datadog, Inc.

use super::pprof::ValueType;
use super::symbol_table::Diff;
use super::u63::u63;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct UnifiedServiceTags {
    env: Option<String>,
    service: Option<String>,
    version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sample {
    locations: Vec<u63>,
    values: Vec<i64>,
    labels: Vec<u63>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Profile {
    // todo: session_id: u64,
    unified_service_tags: UnifiedServiceTags,
    sample_types: Vec<ValueType>,
    start_time: i64,
    period: Option<(i64, ValueType)>,
}

impl Profile {
    pub fn new(
        unified_service_tags: UnifiedServiceTags,
        sample_types: Vec<ValueType>,
        start_time: i64,
        period: Option<(i64, ValueType)>,
    ) -> Self {
        Self {
            unified_service_tags,
            sample_types,
            start_time,
            period,
        }
    }
}

pub struct Record {
    pub profile: Profile,
    pub symbol_diff: Diff,
    pub samples: Vec<Sample>,
}

#[cfg(test)]
mod tests {
    use super::super::{Function, Line, Location, SymbolTable, Transaction};
    use super::*;

    use bumpalo::Bump;
    use std::ops::Range;
    use std::sync::mpsc::channel;

    #[test]
    pub fn send() {
        let arena = Bump::new();
        // Safety: I pinky promise not to reset arena while symbols are alive.
        let mut symbols = unsafe { SymbolTable::new(&arena) };

        let (sender, receiver) = channel::<Record>();

        let join_handle = std::thread::spawn(move || {
            let arena = Bump::new();
            // Safety: I pinky promise not to reset arena while symbols are alive.
            let mut symbols = unsafe { SymbolTable::new(&arena) };

            let record = receiver.recv().unwrap();

            let diff = record.symbol_diff;
            let diff_range = symbols.apply_diff(diff.clone()).unwrap();

            let actual_diff = symbols.fetch_diff(diff_range).unwrap();

            assert_eq!(diff, actual_diff);
        });

        // Transaction begin {{
        let mut transaction = symbols.begin_transaction();
        let wall_samples = transaction.add_string("wall-samples");
        let count = transaction.add_string("count");
        let nanoseconds = transaction.add_string("nanoseconds");

        let str_main = transaction.add_string("main");
        let str_main_c = transaction.add_string("main.c");
        let function_main = transaction.add_function(Function {
            name: str_main.into(),
            filename: str_main_c.into(),
            ..Function::default()
        });

        let location = transaction.add_location(Location {
            lines: vec![Line {
                function_id: function_main.into(),
                line: 4,
            }],
            ..Location::default()
        });

        let diff_range = transaction.save();

        // }}} Transaction end
        let symbol_diff = symbols.fetch_diff(diff_range).unwrap();

        let unified_service_tags = UnifiedServiceTags {
            env: None,
            service: None,
            version: None,
        };

        let sample_types = vec![ValueType {
            r#type: wall_samples.into(),
            unit: count.into(),
        }];
        let profile = Profile::new(unified_service_tags, sample_types, 0, None);

        let samples = vec![Sample {
            locations: vec![location],
            values: vec![1],
            labels: vec![],
        }];

        sender
            .send(Record {
                profile: profile,
                symbol_diff,
                samples,
            })
            .unwrap();

        join_handle.join().unwrap()
    }
}
