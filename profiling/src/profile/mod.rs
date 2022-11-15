// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

pub mod api;
pub mod pprof;
pub mod profiled_endpoints;

use core::fmt;
use std::borrow::{Borrow, Cow};
use std::convert::TryInto;
use std::hash::Hash;
use std::ops::AddAssign;
use std::time::{Duration, SystemTime};

use indexmap::{IndexMap, IndexSet};
use pprof::{Function, Label, Line, Location, ValueType};
use profiled_endpoints::ProfiledEndpointsStats;
use prost::{EncodeError, Message};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct PProfId(usize);

impl From<&PProfId> for u64 {
    fn from(id: &PProfId) -> Self {
        id.0 as u64
    }
}

impl From<PProfId> for u64 {
    fn from(id: PProfId) -> Self {
        id.0.try_into().unwrap_or(0)
    }
}

impl From<&PProfId> for i64 {
    fn from(value: &PProfId) -> Self {
        value.0.try_into().unwrap_or(0)
    }
}

impl From<PProfId> for i64 {
    fn from(value: PProfId) -> Self {
        value.0.try_into().unwrap_or(0)
    }
}

#[derive(Eq, PartialEq, Hash)]
struct Mapping {
    /// Address at which the binary (or DLL) is loaded into memory.
    pub memory_start: u64,
    /// The limit of the address range occupied by this mapping.
    pub memory_limit: u64,
    /// Offset in the binary that corresponds to the first mapped address.
    pub file_offset: u64,

    /// The object this entry is loaded from.  This can be a filename on
    /// disk for the main binary and shared libraries, or virtual
    /// abstractions like "[vdso]".
    pub filename: i64,

    /// A string that uniquely identifies a particular program version
    /// with high probability. E.g., for binaries generated by GNU tools,
    /// it could be the contents of the .note.gnu.build-id field.
    pub build_id: i64,
}

#[derive(Eq, PartialEq, Hash)]
struct Sample {
    /// The ids recorded here correspond to a Profile.location.id.
    /// The leaf is at location_id[0].
    pub locations: Vec<PProfId>,

    /// label includes additional context for this sample. It can include
    /// things like a thread id, allocation size, etc
    pub labels: Vec<Label>,
}

pub struct Profile {
    sample_types: Vec<ValueType>,
    samples: IndexMap<Sample, Vec<i64>>,
    mappings: IndexSet<Mapping>,
    locations: IndexSet<Location>,
    functions: IndexSet<Function>,
    strings: IndexSet<String>,
    start_time: SystemTime,
    period: Option<(i64, ValueType)>,
    endpoints: Endpoints,
}

pub struct Endpoints {
    mappings: IndexMap<i64, i64>,
    local_root_span_id_label: i64,
    endpoint_label: i64,
    stats: ProfiledEndpointsStats,
}

pub struct ProfileBuilder<'a> {
    period: Option<api::Period<'a>>,
    sample_types: Vec<api::ValueType<'a>>,
    start_time: Option<SystemTime>,
}

impl<'a> ProfileBuilder<'a> {
    pub fn new() -> Self {
        ProfileBuilder {
            period: None,
            sample_types: vec![],
            start_time: None,
        }
    }

    pub fn period(mut self, period: Option<api::Period<'a>>) -> Self {
        self.period = period;
        self
    }

    pub fn sample_types(mut self, sample_types: Vec<api::ValueType<'a>>) -> Self {
        self.sample_types = sample_types;
        self
    }

    pub fn start_time(mut self, start_time: Option<SystemTime>) -> Self {
        self.start_time = start_time;
        self
    }

    pub fn build(self) -> Profile {
        let mut profile = Profile::new(self.start_time.unwrap_or_else(SystemTime::now));

        profile.sample_types = self
            .sample_types
            .iter()
            .map(|vt| ValueType {
                r#type: profile.intern(vt.r#type),
                unit: profile.intern(vt.unit),
            })
            .collect();

        if let Some(period) = self.period {
            profile.period = Some((
                period.value,
                ValueType {
                    r#type: profile.intern(period.r#type.r#type),
                    unit: profile.intern(period.r#type.unit),
                },
            ));
        };

        profile
    }
}

impl<'a> Default for ProfileBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

trait DedupExt<T: Eq + Hash> {
    fn dedup(&mut self, item: T) -> usize;

    fn dedup_ref<'a, Q>(&mut self, item: &'a Q) -> usize
    where
        T: Eq + Hash + From<&'a Q> + Borrow<Q>,
        Q: Eq + Hash + ?Sized;
}

impl<T: Sized + Hash + Eq> DedupExt<T> for IndexSet<T> {
    fn dedup(&mut self, item: T) -> usize {
        let (id, _) = self.insert_full(item);
        id
    }

    fn dedup_ref<'a, Q>(&mut self, item: &'a Q) -> usize
    where
        T: Eq + Hash + From<&'a Q> + Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        match self.get_index_of(item) {
            Some(index) => index,
            None => {
                let (index, inserted) = self.insert_full(item.into());
                // This wouldn't make any sense; the item couldn't be found so
                // it was inserted but then it already existed? Screams race-
                // -condition to me!
                assert!(inserted);
                index
            }
        }
    }
}

#[derive(Debug)]
pub struct FullError;

impl fmt::Display for FullError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Full")
    }
}

/// Since the ids are index + 1, we need to take 1 off the size. I also want
/// to restrict the maximum to a 32 bit value; we're gathering way too much
/// data if we ever exceed this in a single profile.
const CONTAINER_MAX: usize = (u32::MAX - 1) as usize;

impl std::error::Error for FullError {}

pub struct EncodedProfile {
    pub start: SystemTime,
    pub end: SystemTime,
    pub buffer: Vec<u8>,
    pub endpoints_stats: Box<ProfiledEndpointsStats>,
}

impl Endpoints {
    pub fn new() -> Self {
        Self {
            mappings: Default::default(),
            local_root_span_id_label: Default::default(),
            endpoint_label: Default::default(),
            stats: Default::default(),
        }
    }
}

impl Default for Endpoints {
    fn default() -> Self {
        Self::new()
    }
}

impl Profile {
    /// Creates a profile with `start_time`. Initializes the string table to
    /// include the empty string. All other fields are default.
    pub fn new(start_time: SystemTime) -> Self {
        /* Do not use Profile's default() impl here or it will cause a stack
         * overflow, since that default impl calls this method.
         */
        let mut profile = Self {
            sample_types: vec![],
            samples: Default::default(),
            mappings: Default::default(),
            locations: Default::default(),
            functions: Default::default(),
            strings: Default::default(),
            start_time,
            period: None,
            endpoints: Default::default(),
        };

        profile.intern("");
        profile
    }

    /// Interns the `str` as a string, returning the id in the string table.
    fn intern(&mut self, str: &str) -> i64 {
        // strings are special because the empty string is actually allowed at
        // index 0; most other 0's are reserved and cannot exist
        self.strings
            .dedup_ref(str)
            .try_into()
            .expect("the machine to run out of memory far before this happens")
    }

    pub fn builder<'a>() -> ProfileBuilder<'a> {
        ProfileBuilder::new()
    }

    fn add_mapping(&mut self, mapping: &api::Mapping) -> Result<PProfId, FullError> {
        // todo: do full checks as part of intern/dedup
        if self.strings.len() >= CONTAINER_MAX || self.mappings.len() >= CONTAINER_MAX {
            return Err(FullError);
        }

        let filename = self.intern(mapping.filename);
        let build_id = self.intern(mapping.build_id);

        let index = self.mappings.dedup(Mapping {
            memory_start: mapping.memory_start,
            memory_limit: mapping.memory_limit,
            file_offset: mapping.file_offset,
            filename,
            build_id,
        });

        /* PProf reserves mapping 0 for "no mapping", and it won't let you put
         * one in there with all "zero" data either, so we shift the ids.
         */
        Ok(PProfId(index + 1))
    }

    fn add_function(&mut self, function: &api::Function) -> PProfId {
        let name = self.intern(function.name);
        let system_name = self.intern(function.system_name);
        let filename = self.intern(function.filename);

        let index = self.functions.dedup(Function {
            id: 0,
            name,
            system_name,
            filename,
            start_line: function.start_line,
        });

        /* PProf reserves function 0 for "no function", and it won't let you put
         * one in there with all "zero" data either, so we shift the ids.
         */
        PProfId(index + 1)
    }

    pub fn add(&mut self, sample: api::Sample) -> Result<PProfId, FullError> {
        if sample.values.len() != self.sample_types.len() {
            return Ok(PProfId(0));
        }

        let values = sample.values.clone();
        let labels = sample
            .labels
            .iter()
            .map(|label| {
                let key = self.intern(label.key);
                let str = label.str.map(|s| self.intern(s)).unwrap_or(0);
                let num_unit = label.num_unit.map(|s| self.intern(s)).unwrap_or(0);

                Label {
                    key,
                    str,
                    num: label.num,
                    num_unit,
                }
            })
            .collect();

        let mut locations: Vec<PProfId> = Vec::with_capacity(sample.locations.len());
        for location in sample.locations.iter() {
            let mapping_id = self.add_mapping(&location.mapping)?;
            let lines: Vec<Line> = location
                .lines
                .iter()
                .map(|line| {
                    let function_id = self.add_function(&line.function);
                    Line {
                        function_id: function_id.0 as u64,
                        line: line.line,
                    }
                })
                .collect();

            let index = self.locations.dedup(Location {
                id: 0,
                mapping_id: u64::from(mapping_id),
                address: location.address,
                lines,
                is_folded: location.is_folded,
            });

            /* PProf reserves location 0. Based on this pattern in other
             * situations, this would be "no location", but I'm not sure how
             * this is logical?
             */
            locations.push(PProfId(index + 1))
        }

        let s = Sample { locations, labels };

        let id = match self.samples.get_index_of(&s) {
            None => {
                self.samples.insert(s, values);
                PProfId(self.samples.len())
            }
            Some(index) => {
                let (_, existing_values) =
                    self.samples.get_index_mut(index).expect("index to exist");
                for (a, b) in existing_values.iter_mut().zip(values) {
                    a.add_assign(b)
                }
                PProfId(index + 1)
            }
        };

        Ok(id)
    }

    fn extract_api_sample_types(&self) -> Option<Vec<api::ValueType>> {
        let mut sample_types: Vec<api::ValueType> = Vec::with_capacity(self.sample_types.len());
        for sample_type in self.sample_types.iter() {
            sample_types.push(api::ValueType {
                r#type: self.get_string(sample_type.r#type)?.as_str(),
                unit: self.get_string(sample_type.unit)?.as_str(),
            })
        }
        Some(sample_types)
    }

    /// Resets all data except the sample types and period. Returns the
    /// previous Profile on success.
    pub fn reset(&mut self, start_time: Option<SystemTime>) -> Option<Profile> {
        /* We have to map over the types because the order of the strings is
         * not generally guaranteed, so we can't just copy the underlying
         * structures.
         */
        let sample_types: Vec<api::ValueType> = self.extract_api_sample_types()?;

        let period = match &self.period {
            Some(t) => Some(api::Period {
                r#type: api::ValueType {
                    r#type: self.get_string(t.1.r#type)?.as_str(),
                    unit: self.get_string(t.1.unit)?.as_str(),
                },
                value: t.0,
            }),
            None => None,
        };

        let mut profile = ProfileBuilder::new()
            .sample_types(sample_types)
            .period(period)
            .start_time(start_time)
            .build();

        std::mem::swap(&mut *self, &mut profile);
        Some(profile)
    }

    pub fn add_endpoint(&mut self, local_root_span_id: Cow<str>, endpoint: Cow<str>) {
        if self.endpoints.mappings.is_empty() {
            self.endpoints.local_root_span_id_label = self.intern("local root span id");
            self.endpoints.endpoint_label = self.intern("trace endpoint");
        }

        let interned_span_id = self.intern(local_root_span_id.as_ref());
        let interned_endpoint = self.intern(endpoint.as_ref());

        self.endpoints
            .mappings
            .insert(interned_span_id, interned_endpoint);

        self.endpoints.stats.add_endpoint(endpoint.to_string())
    }

    /// Serialize the aggregated profile, adding the end time and duration.
    /// # Arguments
    /// * `end_time` - Optional end time of the profile. Passing None will use the current time.
    /// * `duration` - Optional duration of the profile. Passing None will try to calculate the
    ///                duration based on the end time minus the start time, but under anomalous
    ///                conditions this may fail as system clocks can be adjusted. The programmer
    ///                may also accidentally pass an earlier time. The duration will be set to zero
    ///                these cases.
    pub fn serialize(
        &self,
        end_time: Option<SystemTime>,
        duration: Option<Duration>,
    ) -> Result<EncodedProfile, EncodeError> {
        let end = end_time.unwrap_or_else(SystemTime::now);
        let start = self.start_time;
        let mut profile: pprof::Profile = self.into();

        profile.duration_nanos = duration
            .unwrap_or_else(|| {
                end.duration_since(start).unwrap_or({
                    // Let's not throw away the whole profile just because the clocks were wrong.
                    // todo: log that the clock went backward (or programmer mistake).
                    Duration::ZERO
                })
            })
            .as_nanos()
            .min(i64::MAX as u128) as i64;

        let mut buffer: Vec<u8> = Vec::new();
        profile.encode(&mut buffer)?;

        Ok(EncodedProfile {
            start,
            end,
            buffer,
            endpoints_stats: Box::new(self.endpoints.stats.clone()),
        })
    }

    pub fn get_string(&self, id: i64) -> Option<&String> {
        self.strings.get_index(id as usize)
    }
}

impl From<&Profile> for pprof::Profile {
    fn from(profile: &Profile) -> Self {
        let (period, period_type) = match profile.period {
            Some(tuple) => (tuple.0, Some(tuple.1)),
            None => (0, None),
        };

        let mut samples: Vec<pprof::Sample> = profile
            .samples
            .iter()
            .map(|(sample, values)| pprof::Sample {
                location_ids: sample.locations.iter().map(Into::into).collect(),
                values: values.to_vec(),
                labels: sample.labels.clone(),
            })
            .collect();

        if !profile.endpoints.mappings.is_empty() {
            for sample in samples.iter_mut() {
                let mut endpoint: Option<&i64> = None;

                for label in &sample.labels {
                    if label.key == profile.endpoints.local_root_span_id_label {
                        endpoint = profile.endpoints.mappings.get(&label.str);
                        break;
                    }
                }

                if let Some(endpoint_value) = endpoint {
                    sample.labels.push(pprof::Label {
                        key: profile.endpoints.endpoint_label,
                        str: *endpoint_value,
                        num: 0,
                        num_unit: 0,
                    });
                }
            }
        }

        pprof::Profile {
            sample_types: profile.sample_types.clone(),
            samples,
            mappings: profile
                .mappings
                .iter()
                .enumerate()
                .map(|(index, mapping)| pprof::Mapping {
                    id: (index + 1) as u64,
                    memory_start: mapping.memory_start,
                    memory_limit: mapping.memory_limit,
                    file_offset: mapping.file_offset,
                    filename: mapping.filename,
                    build_id: mapping.build_id,
                    ..Default::default() // todo: support detailed Mapping info
                })
                .collect(),
            locations: profile
                .locations
                .iter()
                .enumerate()
                .map(|(index, location)| pprof::Location {
                    id: (index + 1) as u64,
                    mapping_id: location.mapping_id,
                    address: location.address,
                    lines: location.lines.clone(),
                    is_folded: location.is_folded,
                })
                .collect(),
            functions: profile
                .functions
                .iter()
                .enumerate()
                .map(|(index, function)| {
                    let mut function = *function;
                    function.id = (index + 1) as u64;
                    function
                })
                .collect(),
            string_table: profile.strings.iter().map(Into::into).collect(),
            time_nanos: profile
                .start_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |duration| {
                    duration.as_nanos().min(i64::MAX as u128) as i64
                }),
            period,
            period_type,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod api_test {

    use indexmap::IndexMap;

    use crate::profile::{
        api, pprof, profiled_endpoints::ProfiledEndpointsStats, PProfId, Profile, ValueType,
    };
    use std::borrow::Cow;

    #[test]
    fn interning() {
        let sample_types = vec![api::ValueType {
            r#type: "samples",
            unit: "count",
        }];
        let mut profiles = Profile::builder().sample_types(sample_types).build();

        /* There have been 3 strings: "", "samples", and "count". Since the interning index starts at
         * zero, this means the next string will be 3.
         */
        const EXPECTED_ID: i64 = 3;

        let string = "a";
        let id1 = profiles.intern(string);
        let id2 = profiles.intern(string);

        assert_eq!(id1, id2);
        assert_eq!(id1, EXPECTED_ID);
    }

    #[test]
    fn api() {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let index = api::Function {
            filename: "index.php",
            ..Default::default()
        };

        let locations = vec![
            api::Location {
                mapping,
                lines: vec![api::Line {
                    function: api::Function {
                        name: "phpinfo",
                        system_name: "phpinfo",
                        filename: "index.php",
                        start_line: 0,
                    },
                    line: 0,
                }],
                ..Default::default()
            },
            api::Location {
                mapping,
                lines: vec![api::Line {
                    function: index,
                    line: 3,
                }],
                ..Default::default()
            },
        ];

        let mut profile = Profile::builder().sample_types(sample_types).build();
        let sample_id = profile
            .add(api::Sample {
                locations,
                values: vec![1, 10000],
                labels: vec![],
            })
            .expect("add to succeed");

        assert_eq!(sample_id, PProfId(1));
    }

    fn provide_distinct_locations() -> Profile {
        let sample_types = vec![api::ValueType {
            r#type: "samples",
            unit: "count",
        }];

        let main_lines = vec![api::Line {
            function: api::Function {
                name: "{main}",
                system_name: "{main}",
                filename: "index.php",
                start_line: 0,
            },
            line: 0,
        }];

        let test_lines = vec![api::Line {
            function: api::Function {
                name: "test",
                system_name: "test",
                filename: "index.php",
                start_line: 3,
            },
            line: 0,
        }];

        let mapping = api::Mapping {
            filename: "php",
            ..Default::default()
        };

        let main_locations = vec![api::Location {
            mapping,
            lines: main_lines,
            ..Default::default()
        }];
        let test_locations = vec![api::Location {
            mapping,
            lines: test_lines,
            ..Default::default()
        }];
        let values: Vec<i64> = vec![1];
        let labels = vec![api::Label {
            key: "pid",
            num: 101,
            ..Default::default()
        }];

        let main_sample = api::Sample {
            locations: main_locations,
            values: values.clone(),
            labels: labels.clone(),
        };

        let test_sample = api::Sample {
            locations: test_locations,
            values,
            labels,
        };

        let mut profile = Profile::builder().sample_types(sample_types).build();

        let sample_id1 = profile.add(main_sample).expect("profile to not be full");
        assert_eq!(sample_id1, PProfId(1));

        let sample_id2 = profile.add(test_sample).expect("profile to not be full");
        assert_eq!(sample_id2, PProfId(2));

        profile
    }

    #[test]
    fn impl_from_profile_for_pprof_profile() {
        let locations = provide_distinct_locations();
        let profile = pprof::Profile::from(&locations);

        assert_eq!(profile.samples.len(), 2);
        assert_eq!(profile.mappings.len(), 1);
        assert_eq!(profile.locations.len(), 2);
        assert_eq!(profile.functions.len(), 2);

        for (index, mapping) in profile.mappings.iter().enumerate() {
            assert_eq!((index + 1) as u64, mapping.id);
        }

        for (index, location) in profile.locations.iter().enumerate() {
            assert_eq!((index + 1) as u64, location.id);
        }

        for (index, function) in profile.functions.iter().enumerate() {
            assert_eq!((index + 1) as u64, function.id);
        }

        let sample = profile.samples.get(0).expect("index 0 to exist");
        assert_eq!(sample.labels.len(), 1);
        let label = sample.labels.get(0).expect("index 0 to exist");
        let key = profile
            .string_table
            .get(label.key as usize)
            .expect("index to exist");
        let str = profile
            .string_table
            .get(label.str as usize)
            .expect("index to exist");
        let num_unit = profile
            .string_table
            .get(label.num_unit as usize)
            .expect("index to exist");
        assert_eq!(key, "pid");
        assert_eq!(label.num, 101);
        assert_eq!(str, "");
        assert_eq!(num_unit, "");
    }

    #[test]
    fn reset() {
        let mut profile = provide_distinct_locations();
        /* This set of asserts is to make sure it's a non-empty profile that we
         * are working with so that we can test that reset works.
         */
        assert!(!profile.functions.is_empty());
        assert!(!profile.locations.is_empty());
        assert!(!profile.mappings.is_empty());
        assert!(!profile.samples.is_empty());
        assert!(!profile.sample_types.is_empty());
        assert!(profile.period.is_none());
        assert!(profile.endpoints.mappings.is_empty());
        assert!(profile.endpoints.stats.is_empty());

        let prev = profile.reset(None).expect("reset to succeed");

        // These should all be empty now
        assert!(profile.functions.is_empty());
        assert!(profile.locations.is_empty());
        assert!(profile.mappings.is_empty());
        assert!(profile.samples.is_empty());
        assert!(profile.endpoints.mappings.is_empty());
        assert!(profile.endpoints.stats.is_empty());

        assert_eq!(profile.period, prev.period);
        assert_eq!(profile.sample_types, prev.sample_types);

        // The string table should have at least the empty string:
        assert!(!profile.strings.is_empty());
        // The empty string should be at position 0
        assert_eq!(profile.get_string(0).expect("index 0 to be found"), "");
    }

    #[test]
    fn reset_period() {
        /* The previous test (reset) checked quite a few properties already, so
         * this one will focus only on the period.
         */
        let mut profile = provide_distinct_locations();

        let period = Some((
            10_000_000,
            ValueType {
                r#type: profile.intern("wall-time"),
                unit: profile.intern("nanoseconds"),
            },
        ));
        profile.period = period;

        let prev = profile.reset(None).expect("reset to succeed");
        assert_eq!(period, prev.period);

        // Resolve the string values to check that they match (their string
        // table offsets may not match).
        let (value, period_type) = profile.period.expect("profile to have a period");
        assert_eq!(value, period.unwrap().0);
        assert_eq!(
            profile
                .get_string(period_type.r#type)
                .expect("string to be found"),
            "wall-time"
        );
        assert_eq!(
            profile
                .get_string(period_type.unit)
                .expect("string to be found"),
            "nanoseconds"
        );
    }

    #[test]
    fn lazy_endpoints() {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mut profile: Profile = Profile::builder().sample_types(sample_types).build();

        let id_label = api::Label {
            key: "local root span id",
            str: Some("10"),
            num: 0,
            num_unit: None,
        };

        let id2_label = api::Label {
            key: "local root span id",
            str: Some("11"),
            num: 0,
            num_unit: None,
        };

        let other_label = api::Label {
            key: "other",
            str: Some("test"),
            num: 0,
            num_unit: None,
        };

        let sample1 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id_label, other_label],
        };

        let sample2 = api::Sample {
            locations: vec![],
            values: vec![1, 10000],
            labels: vec![id2_label, other_label],
        };

        profile.add(sample1).expect("add to success");

        profile.add(sample2).expect("add to success");

        profile.add_endpoint(Cow::from("10"), Cow::from("my endpoint"));

        let serialized_profile: pprof::Profile = (&profile).into();

        assert_eq!(serialized_profile.samples.len(), 2);

        let s1 = serialized_profile.samples.get(0).expect("sample");

        // The trace endpoint label should be added to the first sample
        assert_eq!(s1.labels.len(), 3);

        let l1 = s1.labels.get(0).expect("label");

        assert_eq!(
            serialized_profile
                .string_table
                .get(l1.key as usize)
                .unwrap(),
            "local root span id"
        );
        assert_eq!(
            serialized_profile
                .string_table
                .get(l1.str as usize)
                .unwrap(),
            "10"
        );

        let l2 = s1.labels.get(1).expect("label");

        assert_eq!(
            serialized_profile
                .string_table
                .get(l2.key as usize)
                .unwrap(),
            "other"
        );
        assert_eq!(
            serialized_profile
                .string_table
                .get(l2.str as usize)
                .unwrap(),
            "test"
        );

        let l3 = s1.labels.get(2).expect("label");

        assert_eq!(
            serialized_profile
                .string_table
                .get(l3.key as usize)
                .unwrap(),
            "trace endpoint"
        );
        assert_eq!(
            serialized_profile
                .string_table
                .get(l3.str as usize)
                .unwrap(),
            "my endpoint"
        );

        let s2 = serialized_profile.samples.get(1).expect("sample");

        // The trace endpoint label shouldn't be added to second sample because the span id doesn't match
        assert_eq!(s2.labels.len(), 2);
    }

    #[test]
    fn endpoints_count_empty_test() {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let profile: Profile = Profile::builder().sample_types(sample_types).build();

        let encoded_profile = profile
            .serialize(None, None)
            .expect("Unable to encode/serialize the profile");

        let endpoints_stats = &*encoded_profile.endpoints_stats;
        assert!(endpoints_stats.is_empty());
    }

    #[test]
    fn endpoints_count_test() {
        let sample_types = vec![
            api::ValueType {
                r#type: "samples",
                unit: "count",
            },
            api::ValueType {
                r#type: "wall-time",
                unit: "nanoseconds",
            },
        ];

        let mut profile: Profile = Profile::builder().sample_types(sample_types).build();

        let first_local_root_span_id = "1";
        let one_endpoint = "my endpoint";

        profile.add_endpoint(Cow::from(first_local_root_span_id), Cow::from(one_endpoint));

        let second_endpoint = "other endpoint";
        profile.add_endpoint(
            Cow::from(first_local_root_span_id),
            Cow::from(second_endpoint),
        );

        let second_local_root_span_id = "2";
        profile.add_endpoint(
            Cow::from(second_local_root_span_id),
            Cow::from(one_endpoint),
        );

        let encoded_profile = profile
            .serialize(None, None)
            .expect("Unable to encode/serialize the profile");

        let endpoints_stats = &*encoded_profile.endpoints_stats;

        let mut count: IndexMap<String, i64> = IndexMap::new();
        count.insert(one_endpoint.to_string(), 2);
        count.insert(second_endpoint.to_string(), 1);

        let expected_endpoints_stats = ProfiledEndpointsStats::from(count);

        assert_eq!(endpoints_stats, &expected_endpoints_stats);
    }
}
