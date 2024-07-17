// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use crate::trace_utils::{cmp_send_data_payloads, collect_trace_chunks, TracerHeaderTags};
use datadog_trace_protobuf::pb;
use std::cmp::Ordering;

type TracerPayloadV04 = Vec<pb::Span>;

#[derive(Debug, Clone)]
/// Enumerates the different encoding types.
pub enum TraceEncoding {
    /// v0.4 encoding (TracerPayloadV04).
    V04,
    /// v0.7 encoding (TracerPayload).
    V07,
}

#[derive(Debug, Clone)]
/// Enum representing a general abstraction for a collection of tracer payloads.
pub enum TracerPayloadCollection {
    /// Collection of TracerPayloads.
    V07(Vec<pb::TracerPayload>),
    /// Collection of TracerPayloadsV04.
    V04(Vec<TracerPayloadV04>),
}

impl TracerPayloadCollection {
    /// Appends `other` collection of the same type to the current collection.
    ///
    /// #Arguments
    ///
    /// * `other`: collection of the same type.
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use datadog_trace_protobuf::pb::TracerPayload;
    /// use datadog_trace_utils::tracer_payload::TracerPayloadCollection;
    /// let mut col1 = TracerPayloadCollection::V07(vec![TracerPayload::default()]);
    /// let mut col2 = TracerPayloadCollection::V07(vec![TracerPayload::default()]);
    /// col1.append(&mut col2);
    /// ```
    pub fn append(&mut self, other: &mut Self) {
        match self {
            TracerPayloadCollection::V07(dest) => {
                if let TracerPayloadCollection::V07(src) = other {
                    dest.append(src)
                }
            }
            TracerPayloadCollection::V04(dest) => {
                if let TracerPayloadCollection::V04(src) = other {
                    dest.append(src)
                }
            }
        }
    }

    /// Merges traces that came from the same origin together to reduce the payload size.
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use datadog_trace_protobuf::pb::TracerPayload;
    /// use datadog_trace_utils::tracer_payload::TracerPayloadCollection;
    /// let mut col1 =
    ///     TracerPayloadCollection::V07(vec![TracerPayload::default(), TracerPayload::default()]);
    /// col1.merge();
    /// ```
    pub fn merge(&mut self) {
        if let TracerPayloadCollection::V07(collection) = self {
            collection.sort_unstable_by(cmp_send_data_payloads);
            collection.dedup_by(|a, b| {
                if cmp_send_data_payloads(a, b) == Ordering::Equal {
                    // Note: dedup_by drops a, and retains b.
                    b.chunks.append(&mut a.chunks);
                    return true;
                }
                false
            })
        }
    }

    /// Computes the size of the collection.
    ///
    /// # Returns
    ///
    /// The number of traces contained in the collection.
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use datadog_trace_protobuf::pb::TracerPayload;
    /// use datadog_trace_utils::tracer_payload::TracerPayloadCollection;
    /// let col1 = TracerPayloadCollection::V07(vec![TracerPayload::default()]);
    /// col1.size();
    /// ```
    pub fn size(&self) -> usize {
        match self {
            TracerPayloadCollection::V07(collection) => {
                collection.iter().map(|s| s.chunks.len()).sum()
            }
            TracerPayloadCollection::V04(collection) => collection.len(),
        }
    }
}

/// A trait defining custom processing to be applied to `TraceChunks`.
///
/// TraceChunks are part of the v07 Trace payloads. Implementors of this trait can define specific
/// logic to modify or enrich trace chunks and pass it to the `TracerPayloadCollection` via
/// `TracerPayloadParams`.
///
/// # Examples
///
/// Implementing `TraceChunkProcessor` to add a custom tag to each span in a chunk:
///
/// ```rust
/// use datadog_trace_protobuf::pb::{Span, TraceChunk};
/// use datadog_trace_utils::tracer_payload::TraceChunkProcessor;
/// use std::collections::HashMap;
///
/// struct CustomTagProcessor {
///     tag_key: String,
///     tag_value: String,
/// }
///
/// impl TraceChunkProcessor for CustomTagProcessor {
///     fn process(&mut self, chunk: &mut TraceChunk, index: usize) {
///         for span in &mut chunk.spans {
///             span.meta
///                 .insert(self.tag_key.clone(), self.tag_value.clone());
///         }
///     }
/// }
/// ```
pub trait TraceChunkProcessor {
    fn process(&mut self, chunk: &mut pb::TraceChunk, index: usize);
}

#[derive(Default)]
/// Default implementation of `TraceChunkProcessor` that does nothing.
///
/// If used, the compiler should optimize away calls to it.
pub struct DefaultTraceChunkProcessor;

impl TraceChunkProcessor for DefaultTraceChunkProcessor {
    fn process(&mut self, _chunk: &mut pb::TraceChunk, _index: usize) {
        // Default implementation does nothing.
    }
}
/// Represents the parameters required to collect trace chunks from msgpack data.
///
/// This struct encapsulates all the necessary parameters for converting msgpack data into
/// a `TracerPayloadCollection`. It is designed to work with the `TryInto` trait to facilitate
/// the conversion process, handling different encoding types and ensuring that all required
/// data is available for the conversion.
pub struct TracerPayloadParams<'a, T: TraceChunkProcessor + 'a> {
    /// A byte slice containing the serialized msgpack data.
    data: &'a [u8],
    /// Reference to `TracerHeaderTags` containing metadata for the trace.
    tracer_header_tags: &'a TracerHeaderTags<'a>,
    /// A mutable reference to an implementation of `TraceChunkProcessor` that processes each
    /// `TraceChunk` after it is constructed but before it is added to the TracerPayloadCollection.
    /// TraceChunks are only available for v07 traces.
    chunk_processor: &'a mut T,
    /// A boolean indicating whether the agent is running in an agentless mode. This is used to
    /// determine what protocol trace chunks should be serialized into when being sent.
    is_agentless: bool,
    /// The encoding type of the trace data, determining how the data should be deserialized and
    /// processed.
    encoding_type: TraceEncoding,
}

impl<'a, T: TraceChunkProcessor + 'a> TracerPayloadParams<'a, T> {
    pub fn new(
        data: &'a [u8],
        tracer_header_tags: &'a TracerHeaderTags,
        chunk_processor: &'a mut T,
        is_agentless: bool,
        encoding_type: TraceEncoding,
    ) -> TracerPayloadParams<'a, T> {
        TracerPayloadParams {
            data,
            tracer_header_tags,
            chunk_processor,
            is_agentless,
            encoding_type,
        }
    }
}
// TODO: APMSP-1282 - Implement TryInto for other encoding types. Supporting TraceChunkProcessor but
// not supporting v07 is a bit pointless for now.
impl<'a, T: TraceChunkProcessor + 'a> TryInto<TracerPayloadCollection>
    for TracerPayloadParams<'a, T>
{
    type Error = anyhow::Error;
    /// Attempts to convert `TracerPayloadParams` into a `TracerPayloadCollection`.
    ///
    /// This method processes the msgpack data contained within `TracerPayloadParams` based on
    /// the specified `encoding_type`, converting it into a collection of tracer payloads.
    /// The conversion process involves deserializing the msgpack data, applying any necessary
    /// processing through `process_chunk`, and assembling the resulting data into
    /// a `TracerPayloadCollection`.
    ///
    /// Note: Currently only the `TraceEncoding::V04` encoding type is supported.
    ///
    /// # Returns
    ///
    /// A `Result` containing either the successfully converted `TracerPayloadCollection` or
    /// an error if the conversion fails. Possible errors include issues with deserializing the
    /// msgpack data or if the data does not conform to the expected format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use datadog_trace_protobuf::pb;
    /// use datadog_trace_utils::trace_utils::TracerHeaderTags;
    /// use datadog_trace_utils::tracer_payload::{
    ///     DefaultTraceChunkProcessor, TraceEncoding, TracerPayloadCollection, TracerPayloadParams,
    /// };
    /// use std::convert::TryInto;
    ///
    /// let data = &[/* msgpack data */];
    ///
    /// let tracer_header_tags = &TracerHeaderTags::default();
    /// let result: Result<TracerPayloadCollection, _> = TracerPayloadParams::new(
    ///     data,
    ///     tracer_header_tags,
    ///     &mut DefaultTraceChunkProcessor,
    ///     false,
    ///     TraceEncoding::V04,
    /// )
    /// .try_into();
    ///
    /// match result {
    ///     Ok(collection) => println!("Successfully converted to TracerPayloadCollection."),
    ///     Err(e) => println!("Failed to convert: {:?}", e),
    /// }
    /// ```
    fn try_into(self) -> Result<TracerPayloadCollection, Self::Error> {
        match self.encoding_type {
            TraceEncoding::V04 => {
                let traces: Vec<Vec<pb::Span>> = match rmp_serde::from_slice(self.data) {
                    Ok(res) => res,
                    Err(e) => {
                        anyhow::bail!("Error deserializing trace from request body: {e}")
                    }
                };

                if traces.is_empty() {
                    anyhow::bail!("No traces deserialized from the request body.");
                }

                Ok(collect_trace_chunks(
                    traces,
                    self.tracer_header_tags,
                    self.chunk_processor,
                    self.is_agentless,
                    TraceEncoding::V04,
                ))
            }
            _ => todo!("Encodings other than TraceEncoding::V04 not implemented yet."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_span;
    use datadog_trace_protobuf::pb;
    use serde_json::json;
    use std::collections::HashMap;

    fn create_dummy_collection_v07() -> TracerPayloadCollection {
        TracerPayloadCollection::V07(vec![pb::TracerPayload {
            container_id: "".to_string(),
            language_name: "".to_string(),
            language_version: "".to_string(),
            tracer_version: "".to_string(),
            runtime_id: "".to_string(),
            chunks: vec![pb::TraceChunk {
                priority: 0,
                origin: "".to_string(),
                spans: vec![],
                tags: Default::default(),
                dropped_trace: false,
            }],
            tags: Default::default(),
            env: "".to_string(),
            hostname: "".to_string(),
            app_version: "".to_string(),
        }])
    }

    #[test]
    fn test_append_traces_v07() {
        let mut trace = create_dummy_collection_v07();

        let empty = TracerPayloadCollection::V07(vec![]);

        trace.append(&mut trace.clone());
        assert_eq!(2, trace.size());

        trace.append(&mut trace.clone());
        assert_eq!(4, trace.size());

        trace.append(&mut empty.clone());
        assert_eq!(4, trace.size());
    }

    #[test]
    fn test_append_traces_v04() {
        let mut trace =
            TracerPayloadCollection::V04(vec![vec![create_test_span(0, 1, 0, 2, true)]]);

        let empty = TracerPayloadCollection::V04(vec![]);

        trace.append(&mut trace.clone());
        assert_eq!(2, trace.size());

        trace.append(&mut trace.clone());
        assert_eq!(4, trace.size());

        trace.append(&mut empty.clone());
        assert_eq!(4, trace.size());
    }

    #[test]
    fn test_merge_traces() {
        let mut trace = create_dummy_collection_v07();

        trace.append(&mut trace.clone());
        assert_eq!(2, trace.size());

        trace.merge();
        assert_eq!(2, trace.size());
        if let TracerPayloadCollection::V07(collection) = trace {
            assert_eq!(1, collection.len());
        } else {
            panic!("Unexpected type");
        }
    }

    #[test]
    fn test_try_into_success() {
        let span_data1 = json!([{
            "service": "test-service",
            "name": "test-service-name",
            "resource": "test-service-resource",
            "trace_id": 111,
            "span_id": 222,
            "parent_id": 100,
            "start": 1,
            "duration": 5,
            "error": 0,
            "meta": {},
            "metrics": {},
        }]);

        let expected_serialized_span_data1 = vec![pb::Span {
            service: "test-service".to_string(),
            name: "test-service-name".to_string(),
            resource: "test-service-resource".to_string(),
            trace_id: 111,
            span_id: 222,
            parent_id: 100,
            start: 1,
            duration: 5,
            error: 0,
            meta: HashMap::new(),
            metrics: HashMap::new(),
            meta_struct: HashMap::new(),
            r#type: "".to_string(),
            span_links: vec![],
        }];

        let span_data2 = json!([{
            "service": "test-service",
            "name": "test-service-name",
            "resource": "test-service-resource",
            "trace_id": 111,
            "span_id": 333,
            "parent_id": 100,
            "start": 1,
            "duration": 5,
            "error": 1,
            "meta": {},
            "metrics": {},
        }]);

        let expected_serialized_span_data2 = vec![pb::Span {
            service: "test-service".to_string(),
            name: "test-service-name".to_string(),
            resource: "test-service-resource".to_string(),
            trace_id: 111,
            span_id: 333,
            parent_id: 100,
            start: 1,
            duration: 5,
            error: 1,
            meta: HashMap::new(),
            metrics: HashMap::new(),
            meta_struct: HashMap::new(),
            r#type: "".to_string(),
            span_links: vec![],
        }];

        let data = rmp_serde::to_vec(&vec![span_data1, span_data2])
            .expect("Failed to serialize test span.");

        let tracer_header_tags = &TracerHeaderTags::default();

        let result: anyhow::Result<TracerPayloadCollection> = TracerPayloadParams::new(
            &data,
            tracer_header_tags,
            &mut DefaultTraceChunkProcessor,
            false,
            TraceEncoding::V04,
        )
        .try_into();

        assert!(result.is_ok());

        let collection = result.unwrap();
        assert_eq!(2, collection.size());

        if let TracerPayloadCollection::V04(traces) = collection {
            assert_eq!(expected_serialized_span_data1, traces[0]);
            assert_eq!(expected_serialized_span_data2, traces[1]);
        } else {
            panic!("Invalid collection type returned for try_into");
        }
    }
}
