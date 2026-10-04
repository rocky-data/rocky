//! rustls-backed TLS for `tokio-postgres`.
//!
//! `tokio-postgres` takes a pluggable [`MakeTlsConnect`]; this module is a
//! small implementation over `tokio-rustls` so the adapter links no OpenSSL.
//! Two verification policies, matching libpq:
//!
//! - [`Verification::Full`] (`sslmode = verify-full`): the chain is checked
//!   against the Mozilla root set (`webpki-roots`) plus any `sslrootcert`
//!   PEM, and the certificate must name the configured host.
//! - [`Verification::None`] (`sslmode = prefer | require`): the connection
//!   is encrypted but the certificate is not checked — exactly what libpq
//!   does for those modes. Handshake signatures are still verified, so the
//!   peer must hold the private key for the certificate it presents.
//!
//! Channel binding (`tls-server-end-point`) is not offered, so SCRAM falls
//! back to `SCRAM-SHA-256` without `-PLUS`.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};

use crate::connector::PgError;

/// How much of the server certificate to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// Encrypt only (libpq `prefer` / `require`).
    None,
    /// Verify chain and host name (libpq `verify-full`).
    Full,
}

/// The crypto provider every TLS config here is built with. Picked
/// explicitly rather than read from the process default: the workspace links
/// both `ring` and `aws-lc-rs`, and unit tests run without the binary's
/// startup hook that installs one.
fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Build a rustls client config for the given policy.
///
/// # Errors
///
/// Returns [`PgError::Tls`] when `sslrootcert` cannot be read or parsed, or
/// the protocol versions cannot be configured.
pub fn client_config(
    verification: Verification,
    sslrootcert: Option<&str>,
) -> Result<ClientConfig, PgError> {
    let provider = provider();
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| PgError::Tls(e.to_string()))?;
    let config = match verification {
        Verification::Full => {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            if let Some(path) = sslrootcert {
                let certs = CertificateDer::pem_file_iter(path)
                    .map_err(|e| PgError::Tls(format!("cannot read sslrootcert '{path}': {e}")))?;
                let mut added = 0usize;
                for cert in certs {
                    let cert = cert.map_err(|e| {
                        PgError::Tls(format!("cannot parse sslrootcert '{path}': {e}"))
                    })?;
                    roots
                        .add(cert)
                        .map_err(|e| PgError::Tls(format!("invalid sslrootcert '{path}': {e}")))?;
                    added += 1;
                }
                if added == 0 {
                    return Err(PgError::Tls(format!(
                        "sslrootcert '{path}' holds no PEM certificates"
                    )));
                }
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        Verification::None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(EncryptOnlyVerifier {
                algorithms: provider.signature_verification_algorithms,
            }))
            .with_no_client_auth(),
    };
    Ok(config)
}

/// Accepts any certificate chain but still verifies handshake signatures.
#[derive(Debug)]
struct EncryptOnlyVerifier {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for EncryptOnlyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// [`MakeTlsConnect`] over a shared rustls config.
#[derive(Clone)]
pub struct RustlsConnect {
    config: Arc<ClientConfig>,
}

impl RustlsConnect {
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl<S> MakeTlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type TlsConnect = RustlsConnector;
    type Error = io::Error;

    fn make_tls_connect(&mut self, domain: &str) -> io::Result<RustlsConnector> {
        let name = ServerName::try_from(domain.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(RustlsConnector {
            config: Arc::clone(&self.config),
            name,
        })
    }
}

/// One pending TLS handshake.
pub struct RustlsConnector {
    config: Arc<ClientConfig>,
    name: ServerName<'static>,
}

impl<S> TlsConnect<S> for RustlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<RustlsStream<S>>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let connector = tokio_rustls::TlsConnector::from(self.config);
        Box::pin(async move {
            let tls = connector.connect(self.name, stream).await?;
            Ok(RustlsStream(tls))
        })
    }
}

/// An established TLS stream.
pub struct RustlsStream<S>(tokio_rustls::client::TlsStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for RustlsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for RustlsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for RustlsStream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_policies_build() {
        assert!(client_config(Verification::None, None).is_ok());
        assert!(client_config(Verification::Full, None).is_ok());
    }

    #[test]
    fn missing_root_cert_file_is_an_error() {
        let err = client_config(Verification::Full, Some("/nonexistent/root.pem")).unwrap_err();
        assert!(err.to_string().contains("sslrootcert"), "{err}");
    }
}
