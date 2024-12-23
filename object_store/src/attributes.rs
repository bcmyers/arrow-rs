// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;

#[cfg(feature = "local-attributes")]
use serde::{Deserialize, Serialize};

/// Additional object attribute types
#[non_exhaustive]
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
#[cfg_attr(feature = "local-attributes", derive(Serialize, Deserialize))]
pub enum Attribute {
    /// Specifies how the object should be handled by a browser
    ///
    /// See [Content-Disposition](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition)
    ContentDisposition,
    /// Specifies the encodings applied to the object
    ///
    /// See [Content-Encoding](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding)
    ContentEncoding,
    /// Specifies the language of the object
    ///
    /// See [Content-Language](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language)
    ContentLanguage,
    /// Specifies the MIME type of the object
    ///
    /// This takes precedence over any [ClientOptions](crate::ClientOptions) configuration
    ///
    /// See [Content-Type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)
    ContentType,
    /// Overrides cache control policy of the object
    ///
    /// See [Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control)
    CacheControl,
    /// Specifies a user-defined metadata field for the object
    ///
    /// The String is a user-defined key
    Metadata(Cow<'static, str>),
    /// Specifies a provider-specific attribute
    ///
    /// The String is the provider-specific key
    ProviderSpecific(Cow<'static, str>),
}

/// The value of an [`Attribute`]
///
/// Provides efficient conversion from both static and owned strings
///
/// ```
/// # use object_store::AttributeValue;
/// // Can use static strings without needing an allocation
/// let value = AttributeValue::from("bar");
/// // Can also store owned strings
/// let value = AttributeValue::from("foo".to_string());
/// ```
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
#[cfg_attr(feature = "local-attributes", derive(Serialize, Deserialize))]
pub struct AttributeValue(Cow<'static, str>);

impl AsRef<str> for AttributeValue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for AttributeValue {
    fn from(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

impl Deref for AttributeValue {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/// Additional attributes of an object
///
/// Attributes can be specified in [PutOptions](crate::PutOptions) and retrieved
/// from APIs returning [GetResult](crate::GetResult).
///
/// Unlike [`ObjectMeta`](crate::ObjectMeta), [`Attributes`] are not returned by
/// listing APIs
#[derive(Debug, Default, Eq, PartialEq, Clone)]
#[cfg_attr(feature = "local-attributes", derive(Serialize, Deserialize))]
pub struct Attributes(HashMap<Attribute, Option<AttributeValue>>);

impl Attributes {
    /// Create a new empty [`Attributes`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [`Attributes`] with space for `capacity` [`Attribute`]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashMap::with_capacity(capacity))
    }

    /// Insert a new [`Attribute`], [`AttributeValue`] pair
    ///
    /// Returns the previous value for `key` if any
    pub fn insert(
        &mut self,
        key: Attribute,
        value: Option<AttributeValue>,
    ) -> Option<Option<AttributeValue>> {
        self.0.insert(key, value)
    }

    /// Returns the [`AttributeValue`] for `key` if any
    pub fn get(&self, key: &Attribute) -> Option<&Option<AttributeValue>> {
        self.0.get(key)
    }

    /// Removes the [`AttributeValue`] for `key` if any
    pub fn remove(&mut self, key: &Attribute) -> Option<Option<AttributeValue>> {
        self.0.remove(key)
    }

    /// Returns an [`Iterator`] over this
    pub fn iter(&self) -> impl Iterator<Item = (&Attribute, &Option<AttributeValue>)> {
        self.0.iter()
    }

    pub fn iter_set_values(&self) -> impl Iterator<Item = (&Attribute, &AttributeValue)> {
        self.0
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
    }

    /// Returns the number of [`Attribute`] in this collection
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if this contains no [`Attribute`]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K, V> FromIterator<(K, V)> for Attributes
where
    K: Into<Attribute>,
    V: Into<Option<AttributeValue>>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<'a> IntoIterator for &'a Attributes {
    type Item = (&'a Attribute, &'a Option<AttributeValue>);
    type IntoIter = std::collections::hash_map::Iter<'a, Attribute, Option<AttributeValue>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attributes_basic() {
        let mut attributes = Attributes::from_iter([
            (Attribute::ContentDisposition, Some("inline".into())),
            (Attribute::ContentEncoding, Some("gzip".into())),
            (Attribute::ContentLanguage, Some("en-US".into())),
            (Attribute::ContentType, Some("test".into())),
            (Attribute::CacheControl, Some("control".into())),
            (Attribute::Metadata("key1".into()), Some("value1".into())),
        ]);

        assert!(!attributes.is_empty());
        assert_eq!(attributes.len(), 6);

        assert_eq!(
            attributes.get(&Attribute::ContentType),
            Some(&Some("test".into()))
        );

        let metav = Some("control".into());
        assert_eq!(attributes.get(&Attribute::CacheControl), Some(&metav));
        assert_eq!(
            attributes.insert(Attribute::CacheControl, Some("v1".into())),
            Some(metav)
        );
        assert_eq!(attributes.len(), 6);

        assert_eq!(
            attributes.remove(&Attribute::CacheControl).unwrap(),
            Some("v1".into())
        );
        assert_eq!(attributes.len(), 5);

        let metav: Option<AttributeValue> = Some("v2".into());
        attributes.insert(Attribute::CacheControl, metav.clone());
        assert_eq!(attributes.get(&Attribute::CacheControl), Some(&metav));
        assert_eq!(attributes.len(), 6);

        assert_eq!(
            attributes.get(&Attribute::ContentDisposition),
            Some(&Some("inline".into()))
        );
        assert_eq!(
            attributes.get(&Attribute::ContentEncoding),
            Some(&Some("gzip".into()))
        );
        assert_eq!(
            attributes.get(&Attribute::ContentLanguage),
            Some(&Some("en-US".into()))
        );
        assert_eq!(
            attributes.get(&Attribute::Metadata("key1".into())),
            Some(&Some("value1".into()))
        );
    }
}
