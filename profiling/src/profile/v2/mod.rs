pub mod pprof;
mod profile_set;
mod profile_storage;
mod string_table;

pub use pprof::{Function, Label, Line, Location, Mapping, ValueType};
pub use profile_set::*;
pub use profile_storage::*;
use std::collections::HashMap;
pub use string_table::*;

use crate::profile::EncodedProfile;
use anyhow::anyhow;
use indexmap::{IndexMap, IndexSet};
use std::hash::Hash;
use std::ops::{Add, AddAssign};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl PProfIdentifiable for Function {
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
}

impl PProfIdentifiable for Location {
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
}

impl PProfIdentifiable for Mapping {
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
}

pub struct Endpoints {
    mappings: HashMap<i64, i64>,
    local_root_span_id_label: i64,
    endpoint_label: i64,
}

impl Endpoints {
    pub fn new(local_root_span_id_label: i64, endpoint_label: i64) -> Self {
        Self {
            mappings: Default::default(),
            local_root_span_id_label,
            endpoint_label,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }

    /// # Arguments
    /// * `span_id` - Index into the string table.
    /// * `endpoint` - Index into the string table.
    pub fn add(&mut self, span_id: i64, endpoint: i64) {
        self.mappings.insert(span_id, endpoint);
    }

    pub fn find_trace_resource(&self, labels: &Vec<Label>) -> Option<Label> {
        for label in labels.iter() {
            if label.str == self.local_root_span_id_label {
                if let Some(endpoint) = self.mappings.get(&label.str) {
                    return Some(Label::str(self.endpoint_label, *endpoint));
                }
                break;
            }
        }
        None
    }

    pub fn add_trace_resource_label(&self, sample: &mut pprof::Sample) {
        if let Some(label) = self.find_trace_resource(&sample.labels) {
            sample.labels.push(label);
        }
    }
}

pub struct Profile {
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
            let endpoint_label = lock.intern("trace endpoint");
            drop(lock); // release lock as quickly as possible

            Ok(Self {
                string_table,
                sample_types,
                samples,
                time_nanos,
                duration_nanos,
                period_type,
                period,
                endpoints: Endpoints::new(local_root_span_id_label, endpoint_label),
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

    pub fn add_endpoint(&mut self, local_root_span_id: i64, endpoint: i64) {
        self.endpoints.mappings.insert(local_root_span_id, endpoint);
    }

    pub fn into_pprof(mut self, profile_storage: &ProfileStorage) -> pprof::Profile {
        let functions = profile_storage.functions();
        let locations = profile_storage.locations();
        let mappings = profile_storage.mappings();

        let lock = self.string_table.lock();
        let string_table = lock.strings();
        drop(lock);

        // Add endpoint profiling information while converting the samples.
        let endpoints = self.endpoints;
        let samples = if !endpoints.mappings.is_empty() {
            let f = |(mut sample, values): (Sample, Vec<i64>)| {
                for label in sample.labels.iter() {
                    if label.key == endpoints.local_root_span_id_label {
                        if let Some(endpoint) = endpoints.mappings.get(&label.str) {
                            let label = Label::str(endpoints.endpoint_label, *endpoint);
                            sample.labels.push(label);
                        }
                        break;
                    }
                }

                pprof::Sample {
                    location_ids: sample.location_ids,
                    values,
                    labels: sample.labels,
                }
            };

            self.samples.drain(..).map(f).collect()
        } else {
            let f = |(sample, values): (Sample, Vec<i64>)| pprof::Sample {
                location_ids: sample.location_ids,
                values,
                labels: sample.labels,
            };

            self.samples.drain(..).map(f).collect()
        };

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
        profile_storage: &ProfileStorage,
        end_time: SystemTime,
        duration: Option<Duration>,
    ) -> anyhow::Result<EncodedProfile> {
        let start = UNIX_EPOCH.add(Duration::from_nanos(self.time_nanos.try_into().unwrap()));
        let end = if end_time < start { start } else { end_time };

        if let Some(duration) = duration {
            self.duration_nanos = duration.as_nanos().try_into().unwrap_or(i64::MAX);
        } else if self.duration_nanos == 0 {
            // end time is at least start time; checked above
            self.duration_nanos = end_time
                .duration_since(start)
                .unwrap()
                .as_nanos()
                .try_into()
                .unwrap_or(i64::MAX);
        }

        let pprof = self.into_pprof(profile_storage);

        let mut buffer = Vec::new();
        use prost::Message;
        pprof.encode(&mut buffer)?;

        Ok(EncodedProfile { start, end, buffer })
    }
}

#[derive(Eq, PartialEq, Hash)]
pub struct Sample {
    pub location_ids: Vec<u64>,
    pub labels: Vec<Label>,
}
