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

//! Trusted distribution hooks for HTTP admission, execution, and Document storage.
//!
//! The Runner initializes these implementations once after configuration parsing.
//! Only its serialized control loop calls the execution policy. Blocking Store
//! jobs share the protection implementation, while the core retains all file,
//! commit, process, cancellation, and shutdown ownership.

use crate::tenon_document::VerifiedTenonDocument;
use axum::http::{HeaderMap, HeaderValue, Method};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

/// The three implementations installed by one Runner initialization callback.
pub struct RunnerHooks {
    pub(crate) execution_policy: Box<dyn ExecutionPolicy>,
    pub(crate) document_protection: Arc<dyn DocumentProtection>,
    pub(crate) http_authorization: Arc<dyn HttpApiAuthorization>,
}

impl RunnerHooks {
    /// Installs the execution, storage, and HTTP implementations for this Runner lifetime.
    #[must_use]
    pub fn new(
        execution_policy: impl ExecutionPolicy + 'static,
        document_protection: impl DocumentProtection + 'static,
        http_authorization: impl HttpApiAuthorization + 'static,
    ) -> Self {
        Self {
            execution_policy: Box::new(execution_policy),
            document_protection: Arc::new(document_protection),
            http_authorization: Arc::new(http_authorization),
        }
    }
}

impl Default for RunnerHooks {
    fn default() -> Self {
        Self::new(AllowAll, Plaintext, NoHttpAuth)
    }
}

impl fmt::Debug for RunnerHooks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerHooks")
            .finish_non_exhaustive()
    }
}

/// Decides whether the Runner may accept the requested execution scope.
///
/// Calls are synchronous and serialized on the Runner thread; implementations
/// do not need to be `Send` or `Sync`. Implementations must
/// return promptly without blocking I/O. The core supplies verified facts but
/// does not define pricing, quotas, license formats, or hardware identity.
pub trait ExecutionPolicy {
    /// Returns the decision for the proposed state and the current entitlement deadline.
    /// Both allowed and denied decisions update the deadline independently.
    ///
    /// # Errors
    ///
    /// A denied candidate leaves the accepted desired state unchanged. Startup
    /// reports and skips it; an HTTP change returns the rejection to its caller.
    fn authorize(&self, scope: ExecutionScope<'_>) -> Result<ExecutionPermit, ExecutionDenied>;
}

/// Immutable facts borrowed only for one synchronous policy decision.
#[derive(Debug)]
pub struct ExecutionScope<'a> {
    /// The complete desired Document set if this candidate is accepted.
    /// Each id occurs once, including accepted Documents that are not yet ready.
    pub documents: &'a [&'a VerifiedTenonDocument],
}

/// A successful policy decision carrying the current execution deadline.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecutionPermit {
    /// The current entitlement deadline, independent of this operation permission.
    /// `None` grants execution without a time deadline.
    pub entitlement_until: Option<Instant>,
}

/// A redacted policy rejection suitable for a client response or startup diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionDenied {
    /// The current entitlement deadline, independent of this operation rejection.
    /// `None` means no time deadline.
    pub entitlement_until: Option<Instant>,
    code: &'static str,
    message: Box<str>,
}

impl ExecutionDenied {
    /// Creates a stable machine code and a public, non-secret explanation.
    #[must_use]
    pub fn new(
        code: &'static str,
        message: impl Into<Box<str>>,
        entitlement_until: Option<Instant>,
    ) -> Self {
        Self {
            entitlement_until,
            code,
            message: message.into(),
        }
    }

    /// Returns the implementation-selected stable machine code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns the public explanation, without license or key material.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ExecutionDenied {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ExecutionDenied {}

/// Converts complete Document bytes without owning filesystem operations.
///
/// Methods run in blocking Store work. The same implementation is shared by
/// writes and startup recovery. Formats and key material remain implementation
/// details; the core never guesses formats or falls back to plaintext.
pub trait DocumentProtection: Send + Sync {
    /// Writes the protected representation into the core-owned temporary file.
    ///
    /// # Errors
    ///
    /// An error leaves the formal Document unchanged, even after partial output.
    fn protect(&self, source: &[u8], output: &mut dyn Write) -> io::Result<()>;

    /// Writes the recovered original bytes into the core-owned output buffer.
    ///
    /// # Errors
    ///
    /// An error aborts startup; partial output is discarded without parsing.
    fn unprotect(&self, stored: &[u8], output: &mut dyn Write) -> io::Result<()>;
}

/// Decides whether one HTTP request may enter its handler without reading its body.
///
/// Calls borrow the actual method, the percent-encoded path without its query,
/// and the parsed headers including repeated values. Resource-based policies must
/// decode individual path segments once, rather than decode before splitting.
/// Implementations may await I/O but must not block the executor. They own remote
/// deadlines and must release local resources when their future is dropped.
/// HTTP services share one implementation and may call it concurrently; no identity
/// is added to the request. Normal shutdown drains admitted requests; forced
/// cancellation drops pending authorization before any handler side effect.
pub trait HttpApiAuthorization: Send + Sync {
    /// Authorizes one request before body extraction, upload staging, or management work.
    ///
    /// # Errors
    ///
    /// Returns a rejection for this request only. SSE reconnects are checked again;
    /// an established stream is not reauthorized. Execution entitlement is unchanged.
    fn authorize<'a>(
        &'a self,
        method: &'a Method,
        path: &'a str,
        headers: &'a HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpAuthRejection>> + Send + 'a>>;
}

/// An HTTP access rejection rendered by the core using its fixed JSON error format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpAuthRejection {
    /// No valid credentials were supplied; returns HTTP 401.
    Unauthorized {
        /// A valid WWW-Authenticate challenge for the distribution's authentication scheme.
        challenge: HeaderValue,
    },
    /// Access is not permitted; returns HTTP 403.
    Forbidden {
        /// An optional WWW-Authenticate challenge, required by schemes such as Bearer.
        challenge: Option<HeaderValue>,
    },
}

/// The default HTTP policy, which allows every request without checking credentials.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoHttpAuth;

impl HttpApiAuthorization for NoHttpAuth {
    fn authorize<'a>(
        &'a self,
        _method: &'a Method,
        _path: &'a str,
        _headers: &'a HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpAuthRejection>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// The open-source policy, which permits every execution scope without expiry.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl ExecutionPolicy for AllowAll {
    fn authorize(&self, _scope: ExecutionScope<'_>) -> Result<ExecutionPermit, ExecutionDenied> {
        Ok(ExecutionPermit::default())
    }
}

/// The open-source protection implementation, preserving exact original bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Plaintext;

impl DocumentProtection for Plaintext {
    fn protect(&self, source: &[u8], output: &mut dyn Write) -> io::Result<()> {
        output.write_all(source)
    }

    fn unprotect(&self, stored: &[u8], output: &mut dyn Write) -> io::Result<()> {
        output.write_all(stored)
    }
}
