pub mod pprof;
mod profile_storage;
mod string_table;

pub use pprof::{Function, Label, Line, Location, Mapping, ValueType};
pub use profile_storage::*;
pub use string_table::*;

use crate::profile::EncodedProfile;
use anyhow::anyhow;
use indexmap::{IndexMap, IndexSet};
use std::hash::Hash;
use std::ops::{Add, AddAssign};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

pub struct Endpoints {
    mappings: IndexMap<i64, i64>,
    local_root_span_id_label: i64,
    endpoint_label: i64,
}

pub struct Profile {
    storage: Arc<LockedProfileStorage>,
    string_table: Arc<LockedStringTable>,
    sample_types: Vec<ValueType>,
    samples: IndexMap<Sample, Vec<i64>>,
    time_nanos: i64,
    duration_nanos: i64,
    period_type: Option<ValueType>,
    period: i64,
    endpoints: Endpoints,
}

impl Profile {
    pub fn new(
        storage: Arc<LockedProfileStorage>,
        string_table: Arc<LockedStringTable>,
        sample_types: Vec<ValueType>,
        samples: IndexMap<Sample, Vec<i64>>,
        time_nanos: i64,
        duration_nanos: i64,
        period: Option<(ValueType, i64)>,
    ) -> anyhow::Result<Self> {
        if time_nanos < 0 {
            Err(anyhow!(
                "Start time of profile cannot be negative ({})",
                time_nanos
            ))
        } else if duration_nanos < 0 {
            Err(anyhow!(
                "Duration of profile cannot be negative ({})",
                duration_nanos
            ))
        } else {
            let (period_type, period) = if let Some((value_type, value)) = period {
                (Some(value_type), value)
            } else {
                (None, 0)
            };

            let mut lock = string_table.lock();
            let local_root_span_id_label = lock.intern("local root span id");
            let endpoint_label = lock.intern("trace resource");
            drop(lock); // release lock as quickly as possible

            Ok(Self {
                storage,
                string_table,
                sample_types,
                samples,
                time_nanos,
                duration_nanos,
                period_type,
                period,
                endpoints: Endpoints {
                    mappings: IndexMap::new(),
                    local_root_span_id_label,
                    endpoint_label,
                },
            })
        }
    }

    pub fn add_sample(&mut self, sample: Sample, values: Vec<i64>) -> anyhow::Result<()> {
        if self.sample_types.len() != values.len() {
            return Err(anyhow!(
                "Expected {} sample values, received {}.",
                self.sample_types.len(),
                values.len()
            ));
        }

        match self.samples.get_index_of(&sample) {
            None => {
                self.samples.insert(sample, values);
            }
            Some(index) => {
                let (_, existing_values) =
                    self.samples.get_index_mut(index).expect("index to exist");
                for (a, b) in existing_values.iter_mut().zip(values) {
                    a.add_assign(b)
                }
            }
        };

        Ok(())
    }

    pub fn add_mapping(&self, mapping: Mapping) -> u64 {
        self.storage.lock().add_mapping(mapping)
    }

    pub fn add_location(&self, location: Location) -> u64 {
        self.storage.lock().add_location(location)
    }

    pub fn add_function(&self, function: Function) -> u64 {
        self.storage.lock().add_function(function)
    }

    pub fn add_endpoint(&mut self, local_root_span_id: i64, endpoint: i64) {
        self.endpoints.mappings.insert(local_root_span_id, endpoint);
    }

    pub fn into_pprof(mut self) -> pprof::Profile {
        let lock = self.storage.lock();
        let functions = lock.functions();
        let locations = lock.locations();
        let mappings = lock.mappings();
        drop(lock);

        let lock = self.string_table.lock();
        let string_table = lock.strings();
        drop(lock);

        let samples = self
            .samples
            .drain(..)
            .map(|(sample, values)| {
                pprof::Sample {
                    location_ids: sample.location_ids,
                    values,
                    labels: sample.labels, // todo: add endpoint info
                }
            })
            .collect();

        pprof::Profile {
            sample_types: self.sample_types,
            samples,
            mappings,
            locations,
            functions,
            string_table,
            drop_frames: 0,
            keep_frames: 0,
            time_nanos: self.time_nanos,
            duration_nanos: self.duration_nanos,
            period_type: self.period_type,
            period: self.period,
            comment: vec![],
            default_sample_type: 0,
        }
    }

    /// Serialize the profile. If the duration is not None, then the profile's
    /// duration_nanos will be set from the provided duration; otherwise if the
    /// duration_nanos is zero, it the duration will be calculated from the
    /// start and end times.
    pub fn serialize(
        mut self,
        mut end_time_nanos: i64,
        duration: Option<Duration>,
    ) -> anyhow::Result<EncodedProfile> {
        if end_time_nanos < self.time_nanos {
            // todo: how to warn about this?
            end_time_nanos = self.time_nanos;
        }

        if let Some(duration) = duration {
            self.duration_nanos = duration.as_nanos().try_into().unwrap_or(i64::MAX);
        } else if self.duration_nanos == 0 {
            let duration = end_time_nanos - self.time_nanos;
            self.duration_nanos = duration;
        }

        let start = UNIX_EPOCH.add(Duration::from_nanos(self.time_nanos.try_into().unwrap()));
        let end = UNIX_EPOCH.add(Duration::from_nanos(end_time_nanos.try_into().unwrap()));

        use prost::Message;
        let pprof = self.into_pprof();
        let mut buffer = Vec::new();
        pprof.encode(&mut buffer)?;

        Ok(EncodedProfile { start, end, buffer })
    }
}

impl From<Profile> for pprof::Profile {
    fn from(value: Profile) -> Self {
        value.into_pprof()
    }
}

#[derive(Eq, PartialEq, Hash)]
pub struct Sample {
    pub location_ids: Vec<u64>,
    pub labels: Vec<Label>,
}
