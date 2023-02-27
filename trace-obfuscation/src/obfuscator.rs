// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0. This product includes software
// developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present
// Datadog, Inc.

use datadog_trace_protobuf::pb;
use url::Url;

const TAG_HTTP_URL: &str = "http.url";

pub fn obfuscate_span(s: &mut pb::Span) {
    match &s.r#type[..] {
        "web" | "http" => {
            if let Some(url) = s.meta.get_mut(TAG_HTTP_URL) {
                *url = obfuscate_url_string(url, true, true);
            }
        }
        _ => {
            return;
        }
    }
}

// obfuscate_url_string obfuscates the given URL. It must be a valid URL
fn obfuscate_url_string(url: &str, remove_query_string: bool, remove_path_digits: bool) -> String {

    if !remove_query_string && !remove_path_digits {
        return url.to_string();
    }
    let mut parsed_url = match Url::parse(url) {
        Ok(res) => res,
        Err(_) => return "?".to_string(),
    };

    if remove_query_string {
        parsed_url.set_query(None)
    }

    if remove_path_digits {
        let segs: Vec<&str> = match parsed_url.path_segments() {
            Some(res) => res.collect(),
            None => return parsed_url.to_string(),
        };

        let mut processed_path_segs: Vec<&str> = Vec::new();

        for seg in segs {
            if seg.chars().all(char::is_alphabetic) {
                processed_path_segs.push(seg);
            }
        }

        match parsed_url.clone().path_segments_mut() {
            Ok(mut res) => {
                res.clear();
                res.extend(processed_path_segs);
            },
            Err(_) => return "?".to_string(),
        }
    }
    parsed_url.to_string()
}

#[cfg(test)]
mod tests {

    use crate::obfuscator;
    use duplicate::duplicate_item;

    #[duplicate_item(
        [
        test_name   [test_query_string_1]
        input       ["http://foo.com/"]
        expected    ["http://foo.com/"];
        ]
        [
        test_name   [test_query_string_2]
        input       ["http://foo.com/123"]
        expected    ["http://foo.com/123"];
        ]
        [
        test_name   [test_query_string_3]
        input       ["http://foo.com/id/123/page/1?search=bar&page=2"]
        expected    ["http://foo.com/id/123/page/1?"];
        ]
    )]
    #[test]
    fn test_name() {
        let result = obfuscator::obfuscate_url_string(input, true, false);
        assert_eq!(result, expected);
    }
}