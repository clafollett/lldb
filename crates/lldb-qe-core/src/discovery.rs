//! Fleet discovery: turn a handful of *endpoints* into the concrete set of worker URLs behind them.
//!
//! The coordinator is configured with `--workers` — a comma-separated list of Flight endpoints.
//! In a laptop demo those are literal `http://127.0.0.1:50051` addresses, one per worker, and there
//! is nothing to discover. In a real deploy (the CDK stack's ECS service) `--workers` is a *single*
//! Cloud Map DNS name like `http://worker.lldb.local:50051`, and every healthy task in the service
//! registers an A-record under that one name. Querying it returns **all** the task IPs at once.
//!
//! That is the whole trick, and also the whole gap this module fills: a DNS name is a fan-out point,
//! but nothing was expanding it. The coordinator shipped every stage to `workers.first()`, so an ECS
//! service scaled to ten tasks still funneled all work through whichever single IP happened to sort
//! first. Scaling the fleet changed nothing observable.
//!
//! [`discover_workers`] closes that: it resolves each endpoint's host to *all* of its IP addresses
//! and expands it to one `scheme://ip:port` URL per address. A literal IP resolves to itself (one
//! URL); a DNS name resolves to the whole fleet standing behind it. Because resolution happens fresh
//! on every coordinator run, scaling the ECS service from N to M tasks changes the discovered fleet
//! size — and therefore the observed fan-out of the plan — with **no redeploy**. That is the
//! "scaling changes parallelism" property the issue asks for, and it falls out of one DNS query.
//!
//! # Testability
//!
//! Real DNS is not something a unit test should touch. So the enumeration logic is written against
//! an injected resolver ([`discover_with`]): a generic closure `Fn(host:port) -> Future<Vec<SocketAddr>>`.
//! [`discover_workers`] supplies the production resolver (`tokio::net::lookup_host`, which enumerates
//! every A/AAAA record); tests supply a fake map from host to a fixed set of addresses and exercise
//! the parsing, expansion, dedup, and error paths with no network at all. The generic-closure shape
//! needs no `async-trait` and no new dependency — `std::future::Future` is enough.

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;

use anyhow::{Context, Result, bail};

/// Resolve each `--workers` endpoint to the concrete set of worker Flight URLs behind it.
///
/// Each endpoint is a `scheme://host:port` string (scheme `http`/`https`; the port is required —
/// Flight has no default port). The `host` is resolved to every IP address it maps to and expanded
/// to one `scheme://ip:port` URL per address:
///
/// - a literal IP (`http://10.0.0.4:50051`) resolves to itself — one URL;
/// - a DNS name (`http://worker.lldb.local:50051`) resolves to all its A/AAAA records — the whole
///   fleet registered under that name.
///
/// Results are deduped preserving first-seen order (so two endpoints that overlap, or a name that
/// happens to include a literal you also passed, do not double-schedule a worker), and the scheme
/// and port from the endpoint are preserved on every expanded URL.
///
/// Resolution runs on every call, so the returned fleet reflects the service's *current* task set:
/// scaling the fleet changes the number of URLs returned here, and thus the plan's fan-out, without
/// a redeploy.
pub async fn discover_workers(endpoints: &[String]) -> Result<Vec<String>> {
    discover_with(endpoints, |authority| async move {
        // `lookup_host` performs a real DNS (or `/etc/hosts`, or literal-IP) resolution and yields
        // *every* A/AAAA record — which for a Cloud Map service name is one address per healthy task.
        let addrs = tokio::net::lookup_host(&authority)
            .await
            .with_context(|| format!("resolving `{authority}`"))?
            .collect::<Vec<_>>();
        Ok(addrs)
    })
    .await
}

/// The enumeration logic, factored out from real DNS so it is unit-testable.
///
/// `resolve` maps an `"host:port"` authority to the addresses behind it. Production passes a closure
/// backed by `tokio::net::lookup_host`; tests pass a fake that returns fixed addresses, so the parse
/// / expand / dedup / error behavior is exercised without touching the network.
async fn discover_with<F, Fut>(endpoints: &[String], resolve: F) -> Result<Vec<String>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Vec<SocketAddr>>>,
{
    let mut seen = HashSet::new();
    let mut fleet = Vec::new();

    for raw in endpoints {
        let endpoint = Endpoint::parse(raw)?;

        let addrs = resolve(endpoint.authority()).await.with_context(|| {
            format!(
                "discovering workers behind `{raw}` (host `{}`)",
                endpoint.host
            )
        })?;
        if addrs.is_empty() {
            bail!("worker endpoint `{raw}` resolved to no addresses");
        }

        for addr in addrs {
            // `SocketAddr`'s `Display` already renders IPv4 as `ip:port` and IPv6 as `[ip]:port`,
            // and carries the port we asked for, so the scheme + it is a well-formed Flight URL.
            let url = format!("{}://{}", endpoint.scheme, addr);
            if seen.insert(url.clone()) {
                fleet.push(url);
            }
        }
    }

    Ok(fleet)
}

/// A parsed `scheme://host:port` worker endpoint.
struct Endpoint {
    /// `http` or `https`.
    scheme: String,
    /// The host to resolve — a DNS name or a literal IP (possibly IPv6).
    host: String,
    /// The Flight port, preserved onto every expanded URL.
    port: u16,
}

impl Endpoint {
    /// Parse `scheme://host:port` by hand (no `url` crate — a dependency the pins do not want, and
    /// this shape is small enough to read at a glance). IPv6 literal hosts are accepted in the
    /// bracketed form `scheme://[::1]:port`.
    fn parse(raw: &str) -> Result<Self> {
        let (scheme, rest) = raw
            .split_once("://")
            .with_context(|| format!("worker endpoint `{raw}` is missing a `scheme://`"))?;
        if scheme != "http" && scheme != "https" {
            bail!(
                "worker endpoint `{raw}` has unsupported scheme `{scheme}` (expected http or https)"
            );
        }

        // Drop any trailing path — endpoints are bare authorities, but be forgiving of a stray `/`.
        let authority = rest.split('/').next().unwrap_or(rest);

        let (host, port_str) = if let Some(after_bracket) = authority.strip_prefix('[') {
            // Bracketed IPv6 literal: `[::1]:port`.
            let (host, tail) = after_bracket.split_once(']').with_context(|| {
                format!("worker endpoint `{raw}` has an unterminated IPv6 literal (missing `]`)")
            })?;
            let port_str = tail.strip_prefix(':').with_context(|| {
                format!(
                    "worker endpoint `{raw}` is missing a `:port` (Flight needs an explicit port)"
                )
            })?;
            (host.to_string(), port_str)
        } else {
            // `host:port`; `rsplit_once` so a bare (unbracketed) IPv6 would still take the last
            // colon, though callers should bracket those.
            let (host, port_str) = authority.rsplit_once(':').with_context(|| {
                format!(
                    "worker endpoint `{raw}` is missing a `:port` (Flight needs an explicit port)"
                )
            })?;
            (host.to_string(), port_str)
        };

        if host.is_empty() {
            bail!("worker endpoint `{raw}` has an empty host");
        }
        let port: u16 = port_str
            .parse()
            .with_context(|| format!("worker endpoint `{raw}` has an invalid port `{port_str}`"))?;

        Ok(Self {
            scheme: scheme.to_string(),
            host,
            port,
        })
    }

    /// The `"host:port"` string to hand a resolver. IPv6 literal hosts are re-bracketed so
    /// `lookup_host` parses them.
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fake resolver from a `host:port -> [ip:port, ...]` table. It records nothing about
    /// DNS; it just returns whatever the table says, so tests drive the pure expansion logic.
    fn resolver(
        table: Vec<(&'static str, Vec<&'static str>)>,
    ) -> impl Fn(String) -> std::future::Ready<Result<Vec<SocketAddr>>> {
        move |authority: String| {
            let hit = table.iter().find(|(k, _)| *k == authority);
            let result = match hit {
                Some((_, addrs)) => addrs
                    .iter()
                    .map(|a| a.parse::<SocketAddr>().map_err(anyhow::Error::from))
                    .collect::<Result<Vec<_>>>(),
                None => Err(anyhow::anyhow!("no fake record for `{authority}`")),
            };
            std::future::ready(result)
        }
    }

    fn eps(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn dns_name_expands_to_every_address() {
        // One DNS name behind which three tasks are registered → three URLs, same scheme + port.
        let resolve = resolver(vec![(
            "worker.lldb.local:50051",
            vec!["10.0.0.1:50051", "10.0.0.2:50051", "10.0.0.3:50051"],
        )]);
        let fleet = discover_with(&eps(&["http://worker.lldb.local:50051"]), resolve)
            .await
            .unwrap();
        assert_eq!(
            fleet,
            vec![
                "http://10.0.0.1:50051",
                "http://10.0.0.2:50051",
                "http://10.0.0.3:50051",
            ]
        );
    }

    #[tokio::test]
    async fn literal_ip_passes_through_as_one_url() {
        let resolve = resolver(vec![("10.0.0.4:50051", vec!["10.0.0.4:50051"])]);
        let fleet = discover_with(&eps(&["http://10.0.0.4:50051"]), resolve)
            .await
            .unwrap();
        assert_eq!(fleet, vec!["http://10.0.0.4:50051"]);
    }

    #[tokio::test]
    async fn duplicates_across_endpoints_are_deduped_preserving_order() {
        // Two endpoints whose resolutions overlap on 10.0.0.2 — it must appear once, first-seen.
        let resolve = resolver(vec![
            ("a.local:50051", vec!["10.0.0.1:50051", "10.0.0.2:50051"]),
            ("b.local:50051", vec!["10.0.0.2:50051", "10.0.0.3:50051"]),
        ]);
        let fleet = discover_with(
            &eps(&["http://a.local:50051", "http://b.local:50051"]),
            resolve,
        )
        .await
        .unwrap();
        assert_eq!(
            fleet,
            vec![
                "http://10.0.0.1:50051",
                "http://10.0.0.2:50051",
                "http://10.0.0.3:50051",
            ],
            "10.0.0.2 appears once, in first-seen order"
        );
    }

    #[tokio::test]
    async fn scheme_and_port_are_preserved() {
        let resolve = resolver(vec![("secure.local:8443", vec!["10.1.0.1:8443"])]);
        let fleet = discover_with(&eps(&["https://secure.local:8443"]), resolve)
            .await
            .unwrap();
        assert_eq!(fleet, vec!["https://10.1.0.1:8443"]);
    }

    #[tokio::test]
    async fn empty_input_yields_empty() {
        let resolve = resolver(vec![]);
        let fleet = discover_with(&[], resolve).await.unwrap();
        assert!(fleet.is_empty());
    }

    #[tokio::test]
    async fn missing_scheme_errors_clearly() {
        let resolve = resolver(vec![]);
        let err = discover_with(&eps(&["worker.local:50051"]), resolve)
            .await
            .expect_err("no scheme is invalid");
        assert!(err.to_string().contains("scheme://"), "got: {err}");
    }

    #[tokio::test]
    async fn missing_port_errors_clearly() {
        let resolve = resolver(vec![]);
        let err = discover_with(&eps(&["http://worker.local"]), resolve)
            .await
            .expect_err("no port is invalid");
        assert!(err.to_string().contains(":port"), "got: {err}");
    }

    #[tokio::test]
    async fn resolver_error_is_surfaced_with_the_host() {
        // A resolver failure must name the endpoint/host so an operator can tell which one is bad.
        let resolve = resolver(vec![]); // every lookup misses → error
        let err = discover_with(&eps(&["http://broken.lldb.local:50051"]), resolve)
            .await
            .expect_err("resolver failure must surface");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("broken.lldb.local"),
            "error must name the failing host, got: {chain}"
        );
    }

    #[tokio::test]
    async fn ipv6_literal_is_bracketed_and_expanded() {
        // A bracketed IPv6 endpoint parses, and its expanded URL is re-bracketed by SocketAddr.
        let resolve = resolver(vec![("[::1]:50051", vec!["[::1]:50051"])]);
        let fleet = discover_with(&eps(&["http://[::1]:50051"]), resolve)
            .await
            .unwrap();
        assert_eq!(fleet, vec!["http://[::1]:50051"]);
    }
}
