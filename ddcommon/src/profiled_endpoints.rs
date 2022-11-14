// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use std::ops::AddAssign;

use indexmap::IndexMap;
use serde::ser::SerializeMap;
use serde::Serialize;

#[derive(Default, PartialEq, Eq, Debug, Clone)]
pub struct ProfiledEndpointsStats {
    count: IndexMap<String, i64>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ProfiledEndpointStats {
    pub name: String,
    pub count: i64,
}

impl From<ProfiledEndpointsStats> for Vec<ProfiledEndpointStats> {
    fn from(s: ProfiledEndpointsStats) -> Self {
        s.count
            .into_iter()
            .map(|(k, v)| ProfiledEndpointStats { name: k, count: v })
            .collect::<Vec<ProfiledEndpointStats>>()
    }
}

impl From<Vec<ProfiledEndpointStats>> for ProfiledEndpointsStats {
    fn from(stats: Vec<ProfiledEndpointStats>) -> Self {
        let mut map: IndexMap<String, i64> = IndexMap::new();
        for ep in stats {
            map.insert(ep.name, ep.count);
        }
        ProfiledEndpointsStats { count: map }
    }
}

impl Serialize for ProfiledEndpointsStats {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.count.len()))?;
        for (k, v) in &self.count {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl ProfiledEndpointsStats {
    pub fn add_endpoint(&mut self, endpoint_name: String) {
        match self.count.get_index_of(&endpoint_name) {
            None => {
                self.count.insert(endpoint_name.to_string(), 1);
            }
            Some(index) => {
                let (_, current) = self
                    .count
                    .get_index_mut(index)
                    .expect("index does not exist");
                current.add_assign(1);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count.is_empty()
    }
}
