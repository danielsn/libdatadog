// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use crate::Timespec;
use ddcommon_ffi::slice::{AsBytes, CharSlice, Slice};
use ddprof_profiles as profiles;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::str::Utf8Error;
use std::time::{Duration, SystemTime};

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ValueType<'a> {
    pub type_: CharSlice<'a>,
    pub unit: CharSlice<'a>,
}

impl<'a> ValueType<'a> {
    pub fn new(type_: &'a str, unit: &'a str) -> Self {
        Self {
            type_: type_.into(),
            unit: unit.into(),
        }
    }
}

#[repr(C)]
pub struct Period<'a> {
    pub type_: ValueType<'a>,
    pub value: i64,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Label<'a> {
    pub key: CharSlice<'a>,

    /// At most one of the following must be present
    pub str: CharSlice<'a>,
    pub num: i64,

    /// Should only be present when num is present.
    /// Specifies the units of num.
    /// Use arbitrary string (for example, "requests") as a custom count unit.
    /// If no unit is specified, consumer may apply heuristic to deduce the unit.
    /// Consumers may also  interpret units like "bytes" and "kilobytes" as memory
    /// units and units like "seconds" and "nanoseconds" as time units,
    /// and apply appropriate unit conversions to these.
    pub num_unit: CharSlice<'a>,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Function<'a> {
    /// Name of the function, in human-readable form if available.
    pub name: CharSlice<'a>,

    /// Name of the function, as identified by the system.
    /// For instance, it can be a C++ mangled name.
    pub system_name: CharSlice<'a>,

    /// Source file containing the function.
    pub filename: CharSlice<'a>,

    /// Line number in source file.
    pub start_line: i64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Line<'a> {
    /// The corresponding profile.Function for this line.
    pub function: Function<'a>,

    /// Line number in source code.
    pub line: i64,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Location<'a> {
    /// todo: how to handle unknown mapping?
    pub mapping: Mapping<'a>,

    /// The instruction address for this location, if available.  It
    /// should be within [Mapping.memory_start...Mapping.memory_limit]
    /// for the corresponding mapping. A non-leaf address may be in the
    /// middle of a call instruction. It is up to display tools to find
    /// the beginning of the instruction if necessary.
    pub address: u64,

    /// Multiple line indicates this location has inlined functions,
    /// where the last entry represents the caller into which the
    /// preceding entries were inlined.
    ///
    /// E.g., if memcpy() is inlined into printf:
    ///    line[0].function_name == "memcpy"
    ///    line[1].function_name == "printf"
    pub lines: Slice<'a, Line<'a>>,

    /// Provides an indication that multiple symbols map to this location's
    /// address, for example due to identical code folding by the linker. In that
    /// case the line information above represents one of the multiple
    /// symbols. This field must be recomputed when the symbolization state of the
    /// profile changes.
    pub is_folded: bool,
}

#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Mapping<'a> {
    /// Address at which the binary (or DLL) is loaded into memory.
    pub memory_start: u64,

    /// The limit of the address range occupied by this mapping.
    pub memory_limit: u64,

    /// Offset in the binary that corresponds to the first mapped address.
    pub file_offset: u64,

    /// The object this entry is loaded from.  This can be a filename on
    /// disk for the main binary and shared libraries, or virtual
    /// abstractions like "[vdso]".
    pub filename: CharSlice<'a>,

    /// A string that uniquely identifies a particular program version
    /// with high probability. E.g., for binaries generated by GNU tools,
    /// it could be the contents of the .note.gnu.build-id field.
    pub build_id: CharSlice<'a>,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Sample<'a> {
    /// The leaf is at locations[0].
    pub locations: Slice<'a, Location<'a>>,

    /// The type and unit of each value is defined by the corresponding
    /// entry in Profile.sample_type. All samples must have the same
    /// number of values, the same as the length of Profile.sample_type.
    /// When aggregating multiple samples into a single sample, the
    /// result has a list of values that is the element-wise sum of the
    /// lists of the originals.
    pub values: Slice<'a, i64>,

    /// label includes additional context for this sample. It can include
    /// things like a thread id, allocation size, etc
    pub labels: Slice<'a, Label<'a>>,
}

impl<'a> TryFrom<&'a Mapping<'a>> for profiles::api::Mapping<'a> {
    type Error = Utf8Error;

    fn try_from(mapping: &'a Mapping<'a>) -> Result<Self, Self::Error> {
        let filename = unsafe { mapping.filename.try_to_utf8() }?;
        let build_id = unsafe { mapping.build_id.try_to_utf8() }?;
        Ok(Self {
            memory_start: mapping.memory_start,
            memory_limit: mapping.memory_limit,
            file_offset: mapping.file_offset,
            filename,
            build_id,
        })
    }
}

impl<'a> From<&'a ValueType<'a>> for profiles::api::ValueType<'a> {
    fn from(vt: &'a ValueType<'a>) -> Self {
        unsafe {
            Self {
                r#type: vt.type_.try_to_utf8().unwrap_or(""),
                unit: vt.unit.try_to_utf8().unwrap_or(""),
            }
        }
    }
}

impl<'a> From<&'a Period<'a>> for profiles::api::Period<'a> {
    fn from(period: &'a Period<'a>) -> Self {
        Self {
            r#type: profiles::api::ValueType::from(&period.type_),
            value: period.value,
        }
    }
}

impl<'a> TryFrom<&'a Function<'a>> for profiles::api::Function<'a> {
    type Error = Utf8Error;

    fn try_from(function: &'a Function<'a>) -> Result<Self, Self::Error> {
        unsafe {
            let name = function.name.try_to_utf8()?;
            let system_name = function.system_name.try_to_utf8()?;
            let filename = function.filename.try_to_utf8()?;
            Ok(Self {
                name,
                system_name,
                filename,
                start_line: function.start_line,
            })
        }
    }
}

impl<'a> TryFrom<&'a Line<'a>> for profiles::api::Line<'a> {
    type Error = Utf8Error;

    fn try_from(line: &'a Line<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            function: profiles::api::Function::try_from(&line.function)?,
            line: line.line,
        })
    }
}

impl<'a> TryFrom<&'a Location<'a>> for profiles::api::Location<'a> {
    type Error = Utf8Error;

    fn try_from(location: &'a Location<'a>) -> Result<Self, Self::Error> {
        let mapping = profiles::api::Mapping::try_from(&location.mapping)?;
        let mut lines: Vec<profiles::api::Line> = Vec::new();
        unsafe {
            for line in location.lines.as_slice().iter() {
                lines.push(line.try_into()?);
            }
        }
        Ok(Self {
            mapping,
            address: location.address,
            lines,
            is_folded: location.is_folded,
        })
    }
}

impl<'a> TryFrom<&'a Label<'a>> for profiles::api::Label<'a> {
    type Error = Utf8Error;

    fn try_from(label: &'a Label<'a>) -> Result<Self, Self::Error> {
        unsafe {
            let key = label.key.try_to_utf8()?;
            let str = label.str.try_to_utf8()?;
            let str = if str.is_empty() { None } else { Some(str) };
            let num_unit = label.num_unit.try_to_utf8()?;
            let num_unit = if num_unit.is_empty() {
                None
            } else {
                Some(num_unit)
            };

            Ok(Self {
                key,
                str,
                num: label.num,
                num_unit,
            })
        }
    }
}

impl<'a> TryFrom<Sample<'a>> for profiles::api::Sample<'a> {
    type Error = Utf8Error;

    fn try_from(sample: Sample<'a>) -> Result<Self, Self::Error> {
        let mut locations: Vec<profiles::api::Location> =
            Vec::with_capacity(sample.locations.len());
        unsafe {
            for location in sample.locations.as_slice().iter() {
                locations.push(location.try_into()?)
            }

            let values: Vec<i64> = sample.values.into_slice().to_vec();

            let mut labels: Vec<profiles::api::Label> = Vec::with_capacity(sample.labels.len());
            for label in sample.labels.as_slice().iter() {
                labels.push(label.try_into()?);
            }

            Ok(Self {
                locations,
                values,
                labels,
            })
        }
    }
}

/// Create a new profile with the given sample types. Must call
/// `ddog_Profile_free` when you are done with the profile.
///
/// # Arguments
/// * `sample_types`
/// * `period` - Optional period of the profile. Passing None/null translates to zero values.
/// * `start_time` - Optional time the profile started at. Passing None/null will use the current
///                  time.
///
/// # Safety
/// All slices must be have pointers that are suitably aligned for their type
/// and must have the correct number of elements for the slice.
#[no_mangle]
#[must_use]
pub unsafe extern "C" fn ddog_Profile_new(
    sample_types: Slice<ValueType>,
    period: Option<&Period>,
    start_time: Option<&Timespec>,
) -> Box<ddprof_profiles::Profile> {
    let types: Vec<ddprof_profiles::api::ValueType> =
        sample_types.into_slice().iter().map(Into::into).collect();

    let builder = ddprof_profiles::Profile::builder()
        .period(period.map(Into::into))
        .sample_types(types)
        .start_time(start_time.map(SystemTime::from));

    Box::new(builder.build())
}

#[no_mangle]
/// # Safety
/// The `profile` must point to an object created by another FFI routine in this
/// module, such as `ddog_Profile_with_sample_types`.
pub unsafe extern "C" fn ddog_Profile_free(_profile: Box<ddprof_profiles::Profile>) {}

#[no_mangle]
/// # Safety
/// The `profile` ptr must point to a valid Profile object created by this
/// module. All pointers inside the `sample` need to be valid for the duration
/// of this call.
/// This call is _NOT_ thread-safe.
pub extern "C" fn ddog_Profile_add(profile: &mut ddprof_profiles::Profile, sample: Sample) -> u64 {
    match sample.try_into().map(|s| profile.add(s)) {
        Ok(r) => match r {
            Ok(id) => id.into(),
            Err(_) => 0,
        },
        Err(_) => 0,
    }
}

/// Associate an endpoint to a given local root span id.
/// During the serialization of the profile, an endpoint label will be added
/// to all samples that contain a matching local root span id label.
///
/// Note: calling this API causes the "trace endpoint" and "local root span id" strings
/// to be interned, even if no matching sample is found.
///
/// # Arguments
/// * `profile` - a reference to the profile that will contain the samples.
/// * `local_root_span_id` - the value of the local root span id label to look for.
/// * `endpoint` - the value of the endpoint label to add for matching samples.
///
/// # Safety
/// The `profile` ptr must point to a valid Profile object created by this
/// module.
/// This call is _NOT_ thread-safe.
#[no_mangle]
pub unsafe extern "C" fn ddprof_ffi_Profile_set_endpoint<'a>(
    profile: &mut ddprof_profiles::Profile,
    local_root_span_id: CharSlice<'a>,
    endpoint: CharSlice<'a>,
) {
    let local_root_span_id = local_root_span_id.to_utf8_lossy();
    let endpoint = endpoint.to_utf8_lossy();

    profile.add_endpoint(local_root_span_id, endpoint);
}

#[repr(C)]
pub struct EncodedProfile {
    start: Timespec,
    end: Timespec,
    buffer: ddcommon_ffi::Vec<u8>,
}

impl From<ddprof_profiles::EncodedProfile> for EncodedProfile {
    fn from(value: ddprof_profiles::EncodedProfile) -> Self {
        let start = value.start.into();
        let end = value.end.into();
        let buffer = value.buffer.into();
        Self { start, end, buffer }
    }
}

#[repr(C)]
pub enum SerializeResult {
    Ok(EncodedProfile),
    Err(ddcommon_ffi::Vec<u8>),
}

/// Serialize the aggregated profile. Don't forget to clean up the result by
/// calling ddog_SerializeResult_drop.
///
/// # Arguments
/// * `profile` - a reference to the profile being serialized.
/// * `end_time` - optional end time of the profile. If None/null is passed, the current time will
///                be used.
/// * `duration_nanos` - Optional duration of the profile. Passing None or a negative duration will
///                      mean the duration will based on the end time minus the start time, but
///                      under anomalous conditions this may fail as system clocks can be adjusted,
///                      or the programmer accidentally passed an earlier time. The duration of
///                      the serialized profile will be set to zero for these cases.
///
/// # Safety
/// The `profile` must point to a valid profile object.
/// The `end_time` must be null or otherwise point to a valid TimeSpec object.
/// The `duration_nanos` must be null or otherwise point to a valid i64.
#[no_mangle]
pub unsafe extern "C" fn ddog_Profile_serialize(
    profile: &ddprof_profiles::Profile,
    end_time: Option<&Timespec>,
    duration_nanos: Option<&i64>,
) -> SerializeResult {
    let end_time = end_time.map(SystemTime::from);
    let duration = match duration_nanos {
        None => None,
        Some(x) if *x < 0 => None,
        Some(x) => Some(Duration::from_nanos((*x) as u64)),
    };
    match || -> Result<_, Box<dyn Error>> { Ok(profile.serialize(end_time, duration)?) }() {
        Ok(ok) => SerializeResult::Ok(ok.into()),
        Err(err) => SerializeResult::Err(err.into()),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddog_SerializeResult_drop(_result: SerializeResult) {}

#[must_use]
#[no_mangle]
pub unsafe extern "C" fn ddog_Vec_u8_as_slice(vec: &ddcommon_ffi::Vec<u8>) -> Slice<u8> {
    vec.as_slice()
}

/// Resets all data in `profile` except the sample types and period. Returns
/// true if it successfully reset the profile and false otherwise. The profile
/// remains valid if false is returned.
///
/// # Arguments
/// * `profile` - A mutable reference to the profile to be reset.
/// * `start_time` - The time of the profile (after reset). Pass None/null to use the current time.
///
/// # Safety
/// The `profile` must meet all the requirements of a mutable reference to the profile. Given this
/// can be called across an FFI boundary, the compiler cannot enforce this.
/// If `time` is not null, it must point to a valid Timespec object.
#[no_mangle]
pub unsafe extern "C" fn ddog_Profile_reset(
    profile: &mut ddprof_profiles::Profile,
    start_time: Option<&Timespec>,
) -> bool {
    profile.reset(start_time.map(SystemTime::from)).is_some()
}

#[cfg(test)]
mod test {
    use crate::profiles::*;
    use ddcommon_ffi::Slice;

    #[test]
    fn ctor_and_dtor() {
        unsafe {
            let sample_type: *const ValueType = &ValueType::new("samples", "count");
            let profile = ddog_Profile_new(Slice::new(sample_type, 1), None, None);
            ddog_Profile_free(profile);
        }
    }

    #[test]
    fn aggregate_samples() {
        unsafe {
            let sample_type: *const ValueType = &ValueType::new("samples", "count");
            let mut profile = ddog_Profile_new(Slice::new(sample_type, 1), None, None);

            let lines = &vec![Line {
                function: Function {
                    name: "{main}".into(),
                    system_name: "{main}".into(),
                    filename: "index.php".into(),
                    start_line: 0,
                },
                line: 0,
            }];

            let mapping = Mapping {
                filename: "php".into(),
                ..Default::default()
            };

            let locations = vec![Location {
                mapping,
                lines: lines.into(),
                ..Default::default()
            }];
            let values: Vec<i64> = vec![1];
            let labels = vec![Label {
                key: Slice::from("pid"),
                num: 101,
                ..Default::default()
            }];

            let sample = Sample {
                locations: Slice::from(&locations),
                values: Slice::from(&values),
                labels: Slice::from(&labels),
            };

            let aggregator = &mut *profile;

            let sample_id1 = ddog_Profile_add(aggregator, sample);
            assert_eq!(sample_id1, 1);

            let sample_id2 = ddog_Profile_add(aggregator, sample);
            assert_eq!(sample_id1, sample_id2);

            ddog_Profile_free(profile);
        }
    }

    unsafe fn provide_distinct_locations_ffi() -> ddprof_profiles::Profile {
        let sample_type: *const ValueType = &ValueType::new("samples", "count");
        let mut profile = ddog_Profile_new(Slice::new(sample_type, 1), None, None);

        let main_lines = vec![Line {
            function: Function {
                name: "{main}".into(),
                system_name: "{main}".into(),
                filename: "index.php".into(),
                start_line: 0,
            },
            line: 0,
        }];

        let test_lines = vec![Line {
            function: Function {
                name: "test".into(),
                system_name: "test".into(),
                filename: "index.php".into(),
                start_line: 3,
            },
            line: 0,
        }];

        let mapping = Mapping {
            filename: "php".into(),
            ..Default::default()
        };

        let main_locations = vec![Location {
            mapping,
            lines: main_lines.as_slice().into(),
            ..Default::default()
        }];
        let test_locations = vec![Location {
            mapping,
            lines: test_lines.as_slice().into(),
            ..Default::default()
        }];
        let values: Vec<i64> = vec![1];
        let labels = vec![Label {
            key: Slice::from("pid"),
            str: Slice::from(""),
            num: 101,
            num_unit: Slice::from(""),
        }];

        let main_sample = Sample {
            locations: Slice::from(main_locations.as_slice()),
            values: Slice::from(values.as_slice()),
            labels: Slice::from(labels.as_slice()),
        };

        let test_sample = Sample {
            locations: Slice::from(test_locations.as_slice()),
            values: Slice::from(values.as_slice()),
            labels: Slice::from(labels.as_slice()),
        };

        let aggregator = &mut *profile;

        let sample_id1 = ddog_Profile_add(aggregator, main_sample);
        assert_eq!(sample_id1, 1);

        let sample_id2 = ddog_Profile_add(aggregator, test_sample);
        assert_eq!(sample_id2, 2);

        *profile
    }

    #[test]
    fn distinct_locations_ffi() {
        unsafe {
            provide_distinct_locations_ffi();
        }
    }
}
