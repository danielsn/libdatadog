pub mod pprof;
mod profile_storage;
mod string_table;

pub use pprof::*;
pub use profile_storage::*;
pub use string_table::*;

use crate::profile::EncodedProfile;
use anyhow::anyhow;
use indexmap::{IndexMap, IndexSet};
use prost::Message;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Endpoints {
    mappings: IndexMap<i64, i64>,
    local_root_span_id_label: i64,
    endpoint_label: i64,
}

pub struct ProfileBuilder {
    string_table: Arc<LockedStringTable>,
    sample_types: Vec<ValueType>,
    samples: IndexMap<SampleKey, Vec<Breakdown>>,
    label_sets: IndexSet<LabelSet>,
    start_time: SystemTime,
    duration: Duration, // use Duration::ZERO if you don't know
    period_type: Option<ValueType>,
    period: i64,
    endpoints: Endpoints,
}

impl ProfileBuilder {
    pub fn new(
        string_table: Arc<LockedStringTable>,
        sample_types: Vec<ValueType>,
        samples: IndexMap<SampleKey, Vec<Breakdown>>,
        label_sets: IndexSet<LabelSet>,
        start_time: SystemTime,
        duration: Duration,
        period: Option<(ValueType, i64)>,
    ) -> anyhow::Result<Self> {
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
            label_sets,
            start_time,
            duration,
            period_type,
            period,
            endpoints: Endpoints {
                mappings: IndexMap::new(),
                local_root_span_id_label,
                endpoint_label,
            },
        })
    }

    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    fn index_to_id(index: usize) -> u64 {
        (index + 1).try_into().unwrap()
    }

    pub fn add_label_set(&mut self, mut label_set: LabelSet) -> u64 {
        if label_set.labels.is_empty() {
            return 0;
        }
        // Adjust id before inserting it in case it's a new one.
        label_set.id = Self::index_to_id(self.label_sets.len());
        let (index, _) = self.label_sets.insert_full(label_set);
        Self::index_to_id(index)
    }

    pub fn add_sample(
        &mut self,
        sample: SampleKey,
        breakdowns: Vec<Breakdown>,
    ) -> anyhow::Result<()> {
        if self.sample_types.len() != breakdowns.len() {
            return Err(anyhow!(
                "Expected {} sample values, received {}.",
                self.sample_types.len(),
                breakdowns.len()
            ));
        }

        match self.samples.get_index_of(&sample) {
            None => {
                self.samples.insert(sample, breakdowns);
            }
            Some(index) => {
                let (_, existing_values) =
                    self.samples.get_index_mut(index).expect("index to exist");
                for (aggr, new) in existing_values.iter_mut().zip(breakdowns) {
                    aggr.ticks.extend(new.ticks);
                    aggr.values.extend(new.values);
                    aggr.label_set_ids.extend(new.label_set_ids);
                }
            }
        };

        Ok(())
    }

    pub fn add_endpoint(&mut self, local_root_span_id: i64, endpoint: i64) {
        self.endpoints.mappings.insert(local_root_span_id, endpoint);
    }

    fn duration_to_i64_nanos(duration: Duration) -> i64 {
        duration.as_nanos().try_into().unwrap_or(i64::MAX)
    }

    fn find_trace_endpoint_label(labels: &Vec<Label>, endpoints: &Endpoints) -> Option<Label> {
        for label in labels.iter() {
            if label.key == endpoints.local_root_span_id_label {
                if let Some(endpoint) = endpoints.mappings.get(&label.str) {
                    return Some(Label::str(endpoints.endpoint_label, *endpoint));
                }
                break;
            }
        }
        None
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
            let f = |(mut sample, breakdowns): (SampleKey, Vec<Breakdown>)| {
                if let Some(label) = Self::find_trace_endpoint_label(&sample.labels, &endpoints) {
                    sample.labels.push(label);
                }

                Self::convert_samples_to_pprof((sample, breakdowns))
            };

            self.samples.drain(..).map(f).collect()
        } else {
            let f = Self::convert_samples_to_pprof;
            self.samples.drain(..).map(f).collect()
        };

        let time_nanos = Self::duration_to_i64_nanos(
            self.start_time
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
        );

        let duration_nanos = Self::duration_to_i64_nanos(self.duration);

        let label_sets: Vec<LabelSet> = self
            .label_sets
            .into_iter()
            .map(|mut label_set: LabelSet| {
                if let Some(label) = Self::find_trace_endpoint_label(&label_set.labels, &endpoints)
                {
                    label_set.labels.push(label);
                }
                label_set
            })
            .collect();

        let profile = Profile {
            sample_types: self.sample_types,
            samples,
            mappings,
            locations,
            functions,
            string_table,
            drop_frames: 0,
            keep_frames: 0,
            time_nanos,
            duration_nanos,
            period_type: self.period_type,
            period: self.period,
            comment: vec![],
            default_sample_type: 0,
            tick_unit: 0,
            label_sets,
        };

        profile
    }

    fn convert_samples_to_pprof(elem: (SampleKey, Vec<Breakdown>)) -> Sample {
        let (sample, breakdowns) = elem;
        let values = breakdowns
            .iter()
            .map(|breakdown| breakdown.values.iter().sum())
            .collect();
        Sample {
            location_ids: sample.location_ids,
            values,
            labels: sample.labels,
            breakdowns,
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
        let start = self.start_time;
        let end = if end_time < start { start } else { end_time };

        if let Some(duration) = duration {
            self.duration = duration;
        } else if self.duration.is_zero() {
            // end time is at least start time; checked above
            self.duration = end_time.duration_since(start).unwrap();
        }

        let pprof = self.into_pprof(profile_storage);

        let mut buffer = Vec::new();
        pprof.encode(&mut buffer)?;

        Ok(EncodedProfile { start, end, buffer })
    }
}

#[derive(Eq, Hash, PartialEq)]
pub struct SampleKey {
    /// The ids recorded here correspond to a Profile.location.id.
    /// The leaf is at location_id\[0\].
    pub location_ids: Vec<u64>,
    /// label includes additional context for this sample. It can include
    /// things like a thread id, allocation size, etc
    pub labels: Vec<Label>,
}
