/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Shared formatting for complete error chains.

use std::error::Error;
use std::fmt;

/// Renders one error and its linear source chain exactly once.
pub(crate) struct ErrorChain<'a>(pub(crate) &'a (dyn Error + 'static));

impl fmt::Display for ErrorChain<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut current = Some(self.0);
        let mut separator = "";
        while let Some(error) = current {
            write!(formatter, "{separator}{error}")?;
            separator = ": ";
            current = error.source();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorChain;
    use std::error::Error;
    use std::fmt;
    use std::io;

    #[derive(Debug)]
    struct ContextError(io::Error);

    impl fmt::Display for ContextError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("operation failed")
        }
    }

    impl Error for ContextError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn error_chain_renders_each_linear_error_once() {
        let error = ContextError(io::Error::other("root cause"));

        assert_eq!(
            ErrorChain(&error).to_string(),
            "operation failed: root cause"
        );
    }
}
