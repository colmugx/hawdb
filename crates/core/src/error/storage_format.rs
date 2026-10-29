// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Display, Formatter};

/// An intact storage envelope requires a different reader capability.
/// Refusal does not authorize repair, truncation, or database recreation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageFormatError {
    UnsupportedVersion {
        format: &'static str,
        found: u16,
        supported: u16,
    },
    UnsupportedFeature {
        format: &'static str,
        feature: &'static str,
        tag: u64,
    },
}

impl Display for StorageFormatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion {
                format,
                found,
                supported,
            } => write!(
                formatter,
                "unsupported {format} version {found}; this reader supports version {supported}"
            ),
            Self::UnsupportedFeature {
                format,
                feature,
                tag,
            } => write!(
                formatter,
                "unsupported {format} feature {feature} tag {tag}"
            ),
        }
    }
}

impl std::error::Error for StorageFormatError {}

pub(super) fn find_storage_format_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<StorageFormatError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<StorageFormatError>() {
            return Some(error.clone());
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>()
            && let Some(inner) = error.get_ref()
            && let Some(error) = find_storage_format_error(inner)
        {
            return Some(error);
        }
        current = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HawDBError;

    #[test]
    fn storage_wrappers_preserve_compatibility_refusal() {
        for error in [
            StorageFormatError::UnsupportedVersion {
                format: "relational durable codec",
                found: 1,
                supported: 2,
            },
            StorageFormatError::UnsupportedFeature {
                format: "relational schema",
                feature: "materialized view expression",
                tag: 6,
            },
        ] {
            let wrapped = std::io::Error::other(std::io::Error::other(error.clone()));
            assert_eq!(
                HawDBError::from_storage_error_with_context(wrapped, "open failed"),
                HawDBError::UnsupportedStorageFormat(error),
            );
        }
        assert_eq!(
            HawDBError::from_storage_error_with_context(
                std::io::Error::other("checksum mismatch"),
                "open failed",
            ),
            HawDBError::Storage("open failed: checksum mismatch".into()),
        );
    }
}
