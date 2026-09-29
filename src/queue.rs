//! Queue / address configuration and query result types used by several packets.

use crate::message::RoutingType;
use crate::simple_string::SimpleString;

/// Queue attributes, mirroring `org.apache.activemq.artemis.api.core.QueueConfiguration`.
///
/// `None` fields are "not specified" and are encoded with the nullable helpers so the broker
/// applies its address-settings defaults.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueConfiguration {
    pub name: SimpleString,
    pub address: SimpleString,
    pub filter_string: Option<SimpleString>,
    pub durable: bool,
    pub temporary: bool,
    pub auto_created: bool,
    pub routing_type: Option<RoutingType>,
    pub max_consumers: Option<i32>,
    pub purge_on_no_consumers: Option<bool>,
    pub exclusive: Option<bool>,
    pub last_value: Option<bool>,
    pub last_value_key: Option<SimpleString>,
    pub non_destructive: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub group_rebalance: Option<bool>,
    pub group_buckets: Option<i32>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub group_first_key: Option<SimpleString>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
}

impl QueueConfiguration {
    /// A durable queue bound to an address of the same name.
    pub fn new(name: impl Into<SimpleString>) -> Self {
        let name = name.into();
        QueueConfiguration { address: name.clone(), name, durable: true, ..Default::default() }
    }

    pub fn address(mut self, address: impl Into<SimpleString>) -> Self {
        self.address = address.into();
        self
    }

    pub fn routing_type(mut self, rt: RoutingType) -> Self {
        self.routing_type = Some(rt);
        self
    }

    pub fn filter(mut self, filter: impl Into<SimpleString>) -> Self {
        self.filter_string = Some(filter.into());
        self
    }

    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn auto_created(mut self, auto_created: bool) -> Self {
        self.auto_created = auto_created;
        self
    }

    pub fn max_consumers(mut self, n: i32) -> Self {
        self.max_consumers = Some(n);
        self
    }

    pub fn purge_on_no_consumers(mut self, purge: bool) -> Self {
        self.purge_on_no_consumers = Some(purge);
        self
    }

    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = Some(exclusive);
        self
    }

    pub fn last_value(mut self, last_value: bool) -> Self {
        self.last_value = Some(last_value);
        self
    }

    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = Some(auto_delete);
        self
    }

    pub fn ring_size(mut self, ring_size: i64) -> Self {
        self.ring_size = Some(ring_size);
        self
    }
}

/// Result of a queue query (`SESS_QUEUEQUERY_RESP*`), superset of all versions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueQueryResult {
    pub exists: bool,
    pub durable: bool,
    pub temporary: bool,
    pub consumer_count: i32,
    pub message_count: i64,
    pub filter_string: Option<SimpleString>,
    pub address: Option<SimpleString>,
    pub name: Option<SimpleString>,
    // V2
    pub auto_create_queues: bool,
    // V3
    pub auto_created: bool,
    pub purge_on_no_consumers: bool,
    pub routing_type: Option<RoutingType>,
    pub max_consumers: i32,
    pub exclusive: Option<bool>,
    pub last_value: Option<bool>,
    pub default_consumer_window_size: Option<i32>,
    pub last_value_key: Option<SimpleString>,
    pub non_destructive: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub group_rebalance: Option<bool>,
    pub group_buckets: Option<i32>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub group_first_key: Option<SimpleString>,
    pub ring_size: Option<i64>,
    pub enabled: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub configuration_managed: Option<bool>,
}

/// Result of an address (binding) query (`SESS_BINDINGQUERY_RESP*`), superset of all versions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddressQueryResult {
    pub exists: bool,
    pub queue_names: Vec<SimpleString>,
    // V2
    pub auto_create_queues: bool,
    // V3
    pub auto_create_addresses: bool,
    // V4
    pub default_purge_on_no_consumers: bool,
    pub default_max_consumers: i32,
    pub default_exclusive: Option<bool>,
    pub default_last_value: Option<bool>,
    pub default_last_value_key: Option<SimpleString>,
    pub default_non_destructive: Option<bool>,
    pub default_consumers_before_dispatch: Option<i32>,
    pub default_delay_before_dispatch: Option<i64>,
    // V5
    pub supports_multicast: bool,
    pub supports_anycast: bool,
}

/// A transport configuration as carried in `DISCONNECT_V3` (target connector).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransportConfiguration {
    pub name: String,
    pub factory_class_name: String,
    /// Parameters in encoding order. Keys starting with `$.EP.` are "extra properties".
    pub params: Vec<(String, TransportParam)>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransportParam {
    Boolean(bool),
    Int(i32),
    Long(i64),
    String(String),
}
