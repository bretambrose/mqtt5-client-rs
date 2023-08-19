/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate lru;

use lru::LruCache;
use std::num::NonZeroUsize;

use std::collections::HashMap;
use crate::*;

#[cfg_attr(test, derive(Debug, Eq, PartialEq))]
pub struct OutboundAliasResolution {
    pub send_topic : bool,

    pub alias : Option<u16>,
}

pub trait OutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16;

    fn reset_for_new_connection(&mut self, max_aliases : u16);

    fn resolve_topic_alias(&mut self, alias: &Option<u16>, topic: &String) -> OutboundAliasResolution;
}

impl<T: OutboundAliasResolver + ?Sized> OutboundAliasResolver for Box<T> {
    fn get_maximum_alias_value(&self) -> u16 { self.as_ref().get_maximum_alias_value() }

    fn reset_for_new_connection(&mut self, max_aliases : u16) { self.as_mut().reset_for_new_connection(max_aliases); }

    fn resolve_topic_alias(&mut self, alias: &Option<u16>, topic: &String) -> OutboundAliasResolution {
        self.as_mut().resolve_topic_alias(alias, topic)
    }
}

pub struct NullOutboundAliasResolver {

}

impl NullOutboundAliasResolver {
    pub fn new() -> NullOutboundAliasResolver {
        NullOutboundAliasResolver {}
    }
}

impl OutboundAliasResolver for NullOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16 { 0 }

    fn reset_for_new_connection(&mut self, _ : u16) {}

    fn resolve_topic_alias(&mut self, _: &Option<u16>, _: &String) -> OutboundAliasResolution {
        OutboundAliasResolution {
            send_topic: true,
            alias : None
        }
    }
}

pub struct ManualOutboundAliasResolver {
    maximum_alias_value : u16,

    current_aliases : HashMap<u16, String>,
}

impl ManualOutboundAliasResolver {
    pub fn new(maximum_alias_value : u16) -> ManualOutboundAliasResolver {
        ManualOutboundAliasResolver {
            maximum_alias_value,
            current_aliases : HashMap::new(),
        }
    }
}

impl OutboundAliasResolver for ManualOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16 { self.maximum_alias_value }

    fn reset_for_new_connection(&mut self, maximum_alias_value : u16) {
        self.maximum_alias_value = maximum_alias_value;
        self.current_aliases.clear();
    }

    fn resolve_topic_alias(&mut self, alias: &Option<u16>, topic: &String) -> OutboundAliasResolution {
        if let Some(alias_ref_value) = alias {
            let alias_value = *alias_ref_value;
            if let Some(existing_alias) = self.current_aliases.get(alias_ref_value) {
                if *existing_alias == *topic {
                    return  OutboundAliasResolution {
                        send_topic: false,
                        alias : Some(alias_value)
                    };
                }
            }

            if alias_value > 0 && alias_value < self.maximum_alias_value {
                self.current_aliases.insert(alias_value, topic.clone());

                return OutboundAliasResolution{
                    send_topic: true,
                    alias: Some(alias_value)
                };
            }
        }

        OutboundAliasResolution {
            send_topic: true,
            alias : None
        }
    }
}

pub struct LruOutboundAliasResolver {
    maximum_alias_value : u16,

    cache : LruCache<String, u16>
}

impl LruOutboundAliasResolver {
    pub fn new(maximum_alias_value : u16) -> LruOutboundAliasResolver {
        LruOutboundAliasResolver {
            maximum_alias_value,
            cache : LruCache::new(NonZeroUsize::new(maximum_alias_value as usize).unwrap())
        }
    }
}

impl OutboundAliasResolver for LruOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u16 { self.maximum_alias_value }

    fn reset_for_new_connection(&mut self, maximum_alias_value : u16) {
        self.maximum_alias_value = maximum_alias_value;
        self.cache.clear();
    }

    fn resolve_topic_alias(&mut self, _: &Option<u16>, topic: &String) -> OutboundAliasResolution {
        if let Some(cached_alias_value) = self.cache.get(topic) {
            let alias_value = *cached_alias_value;

            self.cache.promote(topic);

            return OutboundAliasResolution{
                send_topic: false,
                alias : Some(alias_value)
            };
        }

        let mut alias_value : u16 = (self.cache.len() + 1) as u16;
        if alias_value > self.maximum_alias_value {
            if let Some((_, recycled_alias)) = self.cache.pop_lru() {
                alias_value = recycled_alias;
            } else {
                panic!("Illegal state in LRU outbound topic alias resolver")
            }
        }

        self.cache.push(topic.clone(), alias_value);

        return OutboundAliasResolution{
            send_topic: true,
            alias : Some(alias_value)
        };
    }
}

pub struct InboundAliasResolver {
    maximum_alias_value: u16,

    current_aliases : HashMap<u16, String>
}

impl InboundAliasResolver {
    pub fn new(maximum_alias_value: u16) -> InboundAliasResolver {
        InboundAliasResolver {
            maximum_alias_value,
            current_aliases : HashMap::new()
        }
    }

    pub(crate) fn reset_for_new_connection(&mut self) {
        self.current_aliases.clear();
    }

    pub(crate) fn resolve_topic_alias(&mut self, alias: &Option<u16>, topic: &mut String) -> Mqtt5Result<()> {
        if let Some(alias_value) = alias {
            if let Some(existing_topic) = self.current_aliases.get(alias_value) {
                *topic = existing_topic.clone();
                return Ok(());
            }

            if *alias_value == 0 || *alias_value > self.maximum_alias_value {
                return Err(Mqtt5Error::InboundTopicAliasNotValid);
            }

            // TODO: consider making this validate the topic itself
            if topic.len() == 0 {
                return Err(Mqtt5Error::InboundTopicAliasNotValid);
            }

            self.current_aliases.insert(*alias_value, topic.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::alias::*;
    use crate::Mqtt5Error;

    #[test]
    fn outbound_topic_alias_null() {
        let mut resolver = NullOutboundAliasResolver::new();

        assert_eq!(resolver.get_maximum_alias_value(), 0);
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});

        resolver.reset_for_new_connection(10);
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});
    }

    #[test]
    fn outbound_topic_alias_manual_invalid() {
        let mut resolver = ManualOutboundAliasResolver::new(20);

        assert_eq!(resolver.get_maximum_alias_value(), 20);
        assert_eq!(resolver.resolve_topic_alias(&Some(0), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});
        assert_eq!(resolver.resolve_topic_alias(&Some(21), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});
        assert_eq!(resolver.resolve_topic_alias(&Some(65535), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});

        resolver.reset_for_new_connection(10);

        assert_eq!(resolver.get_maximum_alias_value(), 10);
        assert_eq!(resolver.resolve_topic_alias(&Some(0), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});
        assert_eq!(resolver.resolve_topic_alias(&Some(11), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});
        assert_eq!(resolver.resolve_topic_alias(&Some(65535), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: None});
    }

    #[test]
    fn outbound_topic_alias_manual_success() {
        let mut resolver = ManualOutboundAliasResolver::new(20);

        assert_eq!(resolver.get_maximum_alias_value(), 20);
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_manual_reset() {
        let mut resolver = ManualOutboundAliasResolver::new(20);

        assert_eq!(resolver.get_maximum_alias_value(), 20);
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(2)});

        resolver.reset_for_new_connection(10);

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &"some/topic".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &"some/topic/2".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});

        assert_eq!(resolver.get_maximum_alias_value(), 10);
    }

    /*
     * lru topic sequence tests
     *  cache size of 2
     *  a, b, c, refer to distinct topics
     *  the 'r' suffix refers to expected alias reuse
     */
    #[test]
    fn outbound_topic_alias_lru_a_ar() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
    }

    #[test]
    fn outbound_topic_alias_lru_b_a_br() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_ar_br() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_c_br_cr_br_cr_a() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_c_a_cr_b() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"c".to_string()), OutboundAliasResolution{send_topic: false, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
    }

    #[test]
    fn outbound_topic_alias_lru_a_b_reset_a_b() {
        let mut resolver = LruOutboundAliasResolver::new(2);

        assert_eq!(resolver.get_maximum_alias_value(), 2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
        resolver.reset_for_new_connection(2);
        assert_eq!(resolver.resolve_topic_alias(&None, &"a".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(1)});
        assert_eq!(resolver.resolve_topic_alias(&None, &"b".to_string()), OutboundAliasResolution{send_topic: true, alias: Some(2)});
    }

    #[test]
    fn inbound_topic_alias_resolve_success() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();
        let mut topic2 = "topic2".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut topic1), Ok(()));
        assert_eq!(topic1, "topic1");

        assert_eq!(resolver.resolve_topic_alias(&Some(10), &mut topic2), Ok(()));
        assert_eq!(topic2, "topic2");

        let mut unresolved_topic1 = "".to_string();
        let mut unresolved_topic2 = "".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut unresolved_topic1), Ok(()));
        assert_eq!(unresolved_topic1, "topic1");

        assert_eq!(resolver.resolve_topic_alias(&Some(10), &mut unresolved_topic2), Ok(()));
        assert_eq!(unresolved_topic2, "topic2");
    }

    #[test]
    fn inbound_topic_alias_resolve_failures() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(0), &mut topic1), Err(Mqtt5Error::InboundTopicAliasNotValid));
        assert_eq!(resolver.resolve_topic_alias(&Some(11), &mut topic1), Err(Mqtt5Error::InboundTopicAliasNotValid));

        let mut empty_topic = "".to_string();
        assert_eq!(resolver.resolve_topic_alias(&Some(2), &mut empty_topic), Err(Mqtt5Error::InboundTopicAliasNotValid));
    }

    #[test]
    fn inbound_topic_alias_resolve_reset() {
        let mut resolver = InboundAliasResolver::new(10);

        let mut topic1 = "topic1".to_string();

        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut topic1), Ok(()));

        let mut empty_topic = "".to_string();
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut empty_topic), Ok(()));
        assert_eq!(empty_topic, "topic1");

        resolver.reset_for_new_connection();
        empty_topic = "".to_string();
        assert_eq!(resolver.resolve_topic_alias(&Some(1), &mut empty_topic), Err(Mqtt5Error::InboundTopicAliasNotValid));
    }
}
