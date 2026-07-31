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
//! # One fleet, or one fleet per warehouse
//!
//! Virtual warehouses ([`crate::warehouse`]) make "the fleet" plural: each warehouse is its own
//! pool of workers, and a query must reach *its* pool and no other. The mechanism is already here
//! — one DNS name that fans out to every task behind it — so a warehouse needs nothing more than
//! its **own** name. [`render_warehouse_endpoint`] turns a template like
//! `http://{warehouse}.lldb.local:50051` plus a warehouse name into that endpoint, which
//! [`discover_workers`] then expands exactly as before.
//!
//! A template rather than a hard-coded pattern because the same substitution has to work in two
//! places that spell DNS differently: Cloud Map registers `<warehouse>.lldb.local`, while a
//! compose network alias is a bare `<warehouse>`. One flag covers both, and the placeholder is
//! validated so a template that forgot it fails at startup instead of quietly routing every
//! warehouse to one pool.
//!
//! # Testability
//!
//! Real DNS is not something a unit test should touch. So the enumeration logic is written against
//! an injected resolver ([`discover_workers_with`]): a generic closure
//! `Fn(host:port) -> Future<Vec<SocketAddr>>`. [`discover_workers`] supplies the production
//! resolver (`tokio::net::lookup_host`, which enumerates every A/AAAA record); tests supply a fake
//! map from host to a fixed set of addresses and exercise the parsing, expansion, dedup, and error
//! paths with no network at all. The generic-closure shape needs no `async-trait` and no new
//! dependency — `std::future::Future` is enough.

use std::borrow::Cow;
use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow, bail};
use url::{ParseError, Url};

use crate::services::REDACTED;

/// The token a warehouse endpoint template must contain, replaced with the warehouse's name.
pub const WAREHOUSE_PLACEHOLDER: &str = "{warehouse}";

/// Default warehouse endpoint template: the Cloud Map name the CDK stack registers each
/// warehouse's ECS service under, in the `lldb.local` private namespace.
///
/// Compose overrides it with `http://{warehouse}:50051`, because a Docker network alias is a bare
/// label with no namespace suffix.
pub const DEFAULT_WAREHOUSE_ENDPOINT: &str = "http://{warehouse}.lldb.local:50051";

/// Render a warehouse's Flight endpoint from a template.
///
/// The result is a *fan-out point*, not a worker: pass it to [`discover_workers`] to get one URL
/// per task in that warehouse. Both failure modes are caught here rather than at query time —
/// a template missing the placeholder (which would route every warehouse to the same pool, the
/// worst kind of bug because it produces correct-looking answers on the wrong compute), and a
/// substitution that does not yield a parseable `scheme://host:port`.
pub fn render_warehouse_endpoint(template: &str, warehouse: &str) -> Result<String> {
    if !template.contains(WAREHOUSE_PLACEHOLDER) {
        bail!(
            "warehouse endpoint template `{template}` does not contain `{WAREHOUSE_PLACEHOLDER}`, \
             so every warehouse would resolve to the same fleet"
        );
    }
    let rendered = template.replace(WAREHOUSE_PLACEHOLDER, warehouse);
    // Parse the result now: a bad template is an operator's typo, and it should fail at startup
    // with the rendered string in hand rather than deep inside a resolver.
    Endpoint::parse(&rendered)
        .with_context(|| format!("rendering warehouse endpoint from template `{template}`"))?;
    Ok(rendered)
}

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
/// Endpoints are parsed with `url::Url` (see `Endpoint`, and #111 for why that is not a hand-rolled
/// split), so the host is normalized — lower-cased, IDN punycoded, percent-decoded — before it is
/// resolved, and an endpoint with no host is refused rather than resolved.
///
/// Resolution runs on every call, so the returned fleet reflects the service's *current* task set:
/// scaling the fleet changes the number of URLs returned here, and thus the plan's fan-out, without
/// a redeploy.
pub async fn discover_workers(endpoints: &[String]) -> Result<Vec<String>> {
    discover_workers_with(endpoints, |authority| async move {
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

/// The enumeration logic, factored out from real DNS so it is testable.
///
/// `resolve` maps an `"host:port"` authority to the addresses behind it. Production passes a closure
/// backed by `tokio::net::lookup_host`; tests pass a fake that returns fixed addresses, so the parse
/// / expand / dedup / error behavior is exercised without touching the network.
///
/// Public because the fake resolver is the only honest way to prove the *warehouse* routing story
/// without a Cloud Map namespace: an integration test stands up N in-process workers, has the fake
/// answer `<warehouse>.lldb.local` with exactly those N addresses — which is precisely what Cloud
/// Map does for an ECS service at `desiredCount: N` — and watches the plan's fan-out follow.
pub async fn discover_workers_with<F, Fut>(endpoints: &[String], resolve: F) -> Result<Vec<String>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Vec<SocketAddr>>>,
{
    let mut seen = HashSet::new();
    let mut fleet = Vec::new();

    for raw in endpoints {
        // Both messages below quote `raw` unredacted, which is safe only because this line
        // succeeded: `Endpoint::parse` refuses an endpoint carrying userinfo, so a `raw` that
        // reaches here provably has none.
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

/// `raw` with any password in its userinfo replaced, for a message that has to name an endpoint.
///
/// Textual rather than [`Url`]-based, and that is the whole reason it exists rather than
/// [`services::redact_url`](crate::services::redact_url) being reused: the messages that most need
/// redacting are the ones where [`Url::parse`] has already **failed**. `user:pass@w1:50051` — an
/// endpoint written without its scheme, which is the commonest `--workers` mistake — is not a URL
/// at all, and `redact_url` answers `<unparseable metadata url (redacted)>`, which drops the host
/// the operator has to see along with the password they must not. Here the host survives.
///
/// The authority is everything after `scheme://` up to the first `/`, `?` or `#`, and the **last**
/// `@` in it opens the host, because a password may itself contain one. A string with no such `@`
/// is returned borrowed and untouched, so the common path allocates nothing.
///
/// Its failure mode is over-redaction, never under: a `//` that is really part of a path puts the
/// authority window in the wrong place and can only blank text that is not a password. Do not
/// "improve" it by parsing first — that is precisely the case it is here to cover.
pub fn redact_endpoint(raw: &str) -> Cow<'_, str> {
    let start = raw.find("//").map_or(0, |i| i + 2);
    let authority = match raw[start..].find(['/', '?', '#']) {
        Some(end) => &raw[start..start + end],
        None => &raw[start..],
    };
    let Some(at) = authority.rfind('@') else {
        return Cow::Borrowed(raw);
    };
    // No `:` in the userinfo means there is no password to hide, and blanking anyway would invent
    // one — a bare `user@host` would render as `user:****@host` and send the reader looking for a
    // credential nobody wrote.
    let Some((name, _)) = authority[..at].split_once(':') else {
        return Cow::Borrowed(raw);
    };
    Cow::Owned(format!(
        "{}{name}:{REDACTED}{}",
        &raw[..start],
        &raw[start + at..]
    ))
}

/// A parsed `scheme://host:port` worker endpoint.
///
/// # One parser, because this module is where the fleet list comes from
///
/// This used to be hand-rolled `split("://")` work, under a comment claiming "no `url` crate — a
/// dependency the pins do not want". That reason was never true: `url` is a direct dependency of
/// this crate already ([`crate::services`] composes connection URLs with it), it is one version
/// tree-wide, and the pin rule is about `arrow`/`datafusion`/`object_store` duplication rather than
/// about crate count. What the second parser *did* cost is that
/// `crates/lldb-qe-core/src/remote.rs` keys a worker's identity — the thing that decides whether
/// two fleet entries are one node — off `Url::parse` of the very strings this module emits. Two
/// parsers for one concept disagree eventually, and by the time a disagreement reaches that dedup
/// the spellings are already distinct strings, so it cannot repair them.
///
/// Sharing one *type* between the two sites is the tempting fix and it is not available here:
/// `lldb-qe-core` depends on `lldb-qe-control`, never the reverse, so the shared thing would have
/// to move down into this crate — a code move, not this change. They also answer different
/// questions: `WorkerIdentity` asks "same node?" (and so folds a default port in), while this asks
/// "is this a usable endpoint?" (and so demands the port be written). What they must agree on is
/// **what an origin is**, and they now do, because both get it from `Url`.
struct Endpoint {
    /// `http` or `https`, lower-cased by `Url` — so `HTTP://…` is the same endpoint as `http://…`.
    scheme: String,
    /// The host to resolve, as `Url` normalizes it: lower-cased, IDN punycoded, percent-decoded,
    /// and an IPv6 literal left **bracketed** (`[::1]`) so it concatenates straight onto `:port`.
    host: String,
    /// The Flight port, preserved onto every expanded URL.
    port: u16,
}

impl Endpoint {
    /// Parse `scheme://host:port` with [`Url`], then apply the two rules Flight adds on top of it:
    /// the scheme must be one we dial, and the port must be written out.
    ///
    /// Do **not** drop the host check on the grounds that `Url` already guarantees a host for a
    /// special scheme — it does, an empty host is a parse error for `http`/`https`, and the scheme
    /// check below would refuse the rest anyway. What it buys is the *message*: an endpoint written
    /// without its scheme parses as a scheme with the rest as a path and no host at all
    /// (`w1:50051` is scheme `w1`, path `50051`), and reporting that as "unsupported scheme `w1`"
    /// sends an operator looking for the wrong bug. The refusal being over-determined is the point
    /// — every URL this module emits has an origin, so `WorkerIdentity` can key all of them, and
    /// never has to fall back to comparing whole strings.
    fn parse(raw: &str) -> Result<Self> {
        // Every message below names `redact_endpoint(raw)` and never `raw`, including the ones that
        // fire before anything has been parsed. Refusing userinfo (just below) is not enough on its
        // own, because the refusal can only see the credentials `Url` *recognizes* as such: a
        // scheme-less `user:pass@w1:50051` — the exact string #124 is about — parses as scheme
        // `user` with `pass@w1:50051` in its path, so `username()` is empty and `password()` is
        // `None` while the password is right there in the text. Redaction covers the shapes the
        // refusal cannot see; the refusal covers the shapes redaction would otherwise have to be
        // trusted about forever.
        let shown = redact_endpoint(raw);
        let url = Url::parse(raw).map_err(|err| match err {
            // `url`'s own words for a string with no scheme are "relative URL without a base",
            // which tells an operator staring at `--workers` nothing.
            ParseError::RelativeUrlWithoutBase => {
                anyhow!(
                    "worker endpoint `{shown}` is missing a `scheme://` (expected http or https)"
                )
            }
            other => anyhow!("worker endpoint `{shown}` is not a URL: {other}"),
        })?;

        // A worker URL has no legitimate userinfo: Flight credentials travel in gRPC metadata
        // (`AUTHORIZATION_HEADER`, `lldb-plan-assertion`), and nothing in the dial path reads it —
        // `tls::dial` hands the string to `Channel::from_shared`, which routes on the origin. So
        // this is a fail-closed case rather than a redaction case: silently ignoring a password
        // leaves it in the fleet list, and every future message naming an endpoint then has to
        // remember not to print it.
        if !url.username().is_empty() || url.password().is_some() {
            bail!(
                "worker endpoint `{shown}` carries `user:password@`, which a worker URL has no use \
                 for — a worker authenticates a caller by `LLDB_FLEET_TOKEN` in gRPC metadata, \
                 never by anything in the URL. Remove the credential from the endpoint."
            );
        }

        let host = url.host_str().with_context(|| {
            format!(
                "worker endpoint `{shown}` has no host — it parses as scheme `{}` with the rest as \
                 a path, which is what a `scheme://` left off does",
                url.scheme()
            )
        })?;
        if url.scheme() != "http" && url.scheme() != "https" {
            bail!(
                "worker endpoint `{shown}` has unsupported scheme `{}` (expected http or https)",
                url.scheme()
            );
        }

        let port = match url.port() {
            Some(port) => port,
            None => Self::written_default_port(raw, &url).with_context(|| {
                format!(
                    "worker endpoint `{shown}` is missing a `:port` (Flight needs an explicit port)"
                )
            })?,
        };

        Ok(Self {
            scheme: url.scheme().to_string(),
            host: host.to_string(),
            port,
        })
    }

    /// The scheme's default port, but only when `raw` actually wrote it.
    ///
    /// `Url` *erases* a port equal to its scheme's default, so `https://w:443` and `https://w` both
    /// report `port() == None`. Those two must not collapse: one is a fleet behind a TLS load
    /// balancer and the other is the commonest `--workers` typo, so folding them together with
    /// `port_or_known_default()` would silently dial `:80` for `http://w` and lose the startup
    /// error that names the mistake, while refusing both would stop resolving a deployment that
    /// works today. The distinction survives only in the text — `Url` has already validated that
    /// text, so all that is left is to ask whether the authority ends in the default port.
    fn written_default_port(raw: &str, url: &Url) -> Option<u16> {
        let default = url.port_or_known_default()?;
        // `Url` trims leading/trailing C0 controls and spaces, and accepts any number of `/` or `\`
        // after a special scheme's `:` — read the text the same way, or ` http://w:443` would parse
        // above and be rejected here.
        let text = raw.trim_matches(|c: char| c <= ' ');
        let after_scheme = text.split_once(':')?.1.trim_start_matches(['/', '\\']);
        let authority = after_scheme.split(['/', '?', '#']).next()?;
        let (_, written) = authority.rsplit_once(':')?;
        (written.parse::<u16>().ok()? == default).then_some(default)
    }

    /// The `"host:port"` string to hand a resolver.
    ///
    /// Nothing re-brackets an IPv6 literal here because [`Self::host`] is already bracketed. The
    /// hand-rolled parser this replaced instead tested `host.contains(':')`, which fired on any
    /// host holding a colon for another reason: `http://user:pass@w:50051` became the authority
    /// `[user:pass@w]:50051`, a fabricated IPv6 literal that resolves to nothing.
    fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
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
        let fleet = discover_workers_with(&eps(&["http://worker.lldb.local:50051"]), resolve)
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
        let fleet = discover_workers_with(&eps(&["http://10.0.0.4:50051"]), resolve)
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
        let fleet = discover_workers_with(
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
        let fleet = discover_workers_with(&eps(&["https://secure.local:8443"]), resolve)
            .await
            .unwrap();
        assert_eq!(fleet, vec!["https://10.1.0.1:8443"]);
    }

    #[tokio::test]
    async fn empty_input_yields_empty() {
        let resolve = resolver(vec![]);
        let fleet = discover_workers_with(&[], resolve).await.unwrap();
        assert!(fleet.is_empty());
    }

    #[tokio::test]
    async fn missing_scheme_errors_clearly() {
        let resolve = resolver(vec![]);
        let err = discover_workers_with(&eps(&["worker.local:50051"]), resolve)
            .await
            .expect_err("no scheme is invalid");
        assert!(err.to_string().contains("scheme://"), "got: {err}");
    }

    #[tokio::test]
    async fn missing_port_errors_clearly() {
        let resolve = resolver(vec![]);
        let err = discover_workers_with(&eps(&["http://worker.local"]), resolve)
            .await
            .expect_err("no port is invalid");
        assert!(err.to_string().contains(":port"), "got: {err}");
    }

    #[tokio::test]
    async fn resolver_error_is_surfaced_with_the_host() {
        // A resolver failure must name the endpoint/host so an operator can tell which one is bad.
        let resolve = resolver(vec![]); // every lookup misses → error
        let err = discover_workers_with(&eps(&["http://broken.lldb.local:50051"]), resolve)
            .await
            .expect_err("resolver failure must surface");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("broken.lldb.local"),
            "error must name the failing host, got: {chain}"
        );
    }

    #[test]
    fn a_warehouse_renders_into_its_own_endpoint() {
        assert_eq!(
            render_warehouse_endpoint(DEFAULT_WAREHOUSE_ENDPOINT, "analytics").unwrap(),
            "http://analytics.lldb.local:50051"
        );
        // The compose spelling: a bare network alias, no namespace suffix.
        assert_eq!(
            render_warehouse_endpoint("http://{warehouse}:50051", "etl").unwrap(),
            "http://etl:50051"
        );
        // Distinct warehouses must never collapse onto one endpoint.
        assert_ne!(
            render_warehouse_endpoint(DEFAULT_WAREHOUSE_ENDPOINT, "small").unwrap(),
            render_warehouse_endpoint(DEFAULT_WAREHOUSE_ENDPOINT, "large").unwrap()
        );
    }

    #[test]
    fn a_template_without_the_placeholder_is_refused() {
        // The dangerous typo: it "works", and every warehouse silently shares one fleet.
        let err = render_warehouse_endpoint("http://worker.lldb.local:50051", "analytics")
            .expect_err("a template with no placeholder must not be accepted");
        assert!(err.to_string().contains(WAREHOUSE_PLACEHOLDER), "{err}");
    }

    #[test]
    fn a_template_that_renders_to_garbage_fails_at_render_time() {
        // No port, no scheme: catch it while the template is still in hand, not inside a resolver.
        for template in ["http://{warehouse}.lldb.local", "{warehouse}:50051"] {
            let err = render_warehouse_endpoint(template, "analytics")
                .expect_err("an unparseable rendering must be rejected");
            let chain = format!("{err:#}");
            assert!(chain.contains(template), "must name the template: {chain}");
        }
    }

    #[tokio::test]
    async fn ipv6_literal_is_bracketed_and_expanded() {
        // A bracketed IPv6 endpoint parses, and its expanded URL is re-bracketed by SocketAddr.
        let resolve = resolver(vec![("[::1]:50051", vec!["[::1]:50051"])]);
        let fleet = discover_workers_with(&eps(&["http://[::1]:50051"]), resolve)
            .await
            .unwrap();
        assert_eq!(fleet, vec!["http://[::1]:50051"]);
    }

    /// Every shape the hand-rolled `split("://")` parser and `url::Url` read differently — the
    /// list that made two parsers for one concept worth removing (#111).
    ///
    /// The comment beside each entry is what the *old* parser did with it. Only the first group is
    /// a loosening; the second is the set of spellings that stop being accepted, and every one of
    /// them was already unresolvable — the old parser handed the resolver a host it could not have
    /// looked up — so nothing that worked stops working.
    #[test]
    fn the_shapes_where_a_hand_rolled_parser_and_url_disagree() {
        let accepted = [
            // Rejected: scheme `HTTP` matched neither literal.
            ("HTTP://w.local:50051", "w.local:50051"),
            // Rejected: scheme ` http`. This is what `LLDB_WORKERS="a, b"` yields once clap has
            // split on the comma, so it is a real operator spelling and not a curiosity.
            (" http://w.local:50051 ", "w.local:50051"),
            // Accepted, but resolved under the case it was written in. DNS does not care; the fleet
            // list does, because a worker's identity is keyed on the *normalized* host.
            ("http://W.LOCAL:50051", "w.local:50051"),
            // Rejected: the port parse was handed `50051?q=1` / `50051#frag`.
            ("http://w.local:50051?q=1", "w.local:50051"),
            ("http://w.local:50051#frag", "w.local:50051"),
            // Rejected: `://` was matched literally, and a special scheme tolerates any number of
            // slashes.
            ("http:/w.local:50051", "w.local:50051"),
            // Handed to the resolver as raw UTF-8 / still percent-encoded; getaddrinfo speaks
            // neither, so both were guaranteed NXDOMAIN.
            ("http://wörker.local:50051", "xn--wrker-jua.local:50051"),
            ("http://w%2Elocal:50051", "w.local:50051"),
            // Unchanged, and the reason `written_default_port` exists: `Url::port()` erases a port
            // equal to the scheme's default, and these must keep resolving.
            ("https://w.local:443", "w.local:443"),
            ("http://w.local:80", "w.local:80"),
            (" https://w.local:443 ", "w.local:443"),
            ("http://[::1]:80", "[::1]:80"),
        ];
        for (raw, authority) in accepted {
            let endpoint = Endpoint::parse(raw).unwrap_or_else(|err| panic!("`{raw}`: {err:#}"));
            assert_eq!(endpoint.authority(), authority, "`{raw}`");
        }

        let refused = [
            // Accepted by the hand-rolled parser as host `user:pass@w.local`, which `authority()`
            // then re-bracketed into the fabricated IPv6 literal `[user:pass@w.local]:50051`. #123
            // made `Url` resolve it correctly, which was the wrong fix on its own: resolving it
            // kept the password in the fleet list and therefore in every message naming it, so
            // #124 refuses it outright. That is the stronger guarantee, and it belongs on this
            // list rather than the one above.
            "http://user:pass@w.local:50051",
            // Accepted with a host holding a character no resolver accepts.
            "http://w local:50051",
            "http://x|y:50051",
            "http://ho%zzst:50051",
            // Accepted as host `w.local:50051`, port 2 — the same re-bracketing bug.
            "http://w.local:50051:2",
            // Accepted, and expanded to `http://[fe80::1%<scope>]:50051`, a URL `Url` cannot parse
            // at all — so the fleet list held an entry the other parser could only key verbatim.
            "http://[fe80::1%eth0]:50051",
            // Unchanged: a port `Url` erased is only a port if it was written.
            "https://w.local",
            "http://w.local",
        ];
        for raw in refused {
            assert!(
                Endpoint::parse(raw).is_err(),
                "`{raw}` must not be accepted as an endpoint"
            );
        }
    }

    /// The property that replaces "two parsers that happen to agree": every endpoint this module
    /// accepts, and every URL it emits, is an **origin** — a `url::Url` with a host — whose scheme,
    /// host and port are exactly what `Url` says they are.
    ///
    /// That is the pin. `WorkerIdentity` in `crates/lldb-qe-core/src/remote.rs` keys a worker on
    /// `Url::parse(url).host_str()` plus `port_or_known_default()`, so a fleet list assembled here
    /// can only be read one way. It cannot import that type — `lldb-qe-core` depends on this crate,
    /// not the reverse — so it asserts against the two calls that type makes.
    #[tokio::test]
    async fn everything_accepted_and_emitted_is_an_origin_url_can_key() {
        let endpoints = eps(&[
            "http://worker.lldb.local:50051",
            "HTTPS://Secure.Local:8443",
            "http://[::1]:50051",
            "http://10.0.0.4:50051",
            "https://balanced.local:443",
        ]);

        for raw in &endpoints {
            let endpoint = Endpoint::parse(raw).unwrap_or_else(|err| panic!("`{raw}`: {err:#}"));
            let keyed = Url::parse(raw).unwrap_or_else(|err| panic!("`{raw}`: {err}"));
            assert_eq!(keyed.scheme(), endpoint.scheme, "`{raw}` scheme");
            assert_eq!(
                keyed.host_str(),
                Some(endpoint.host.as_str()),
                "`{raw}` host"
            );
            assert_eq!(
                keyed.port_or_known_default(),
                Some(endpoint.port),
                "`{raw}` port"
            );
        }

        let resolve = resolver(vec![
            ("worker.lldb.local:50051", vec!["10.0.0.1:50051"]),
            ("secure.local:8443", vec!["10.0.0.2:8443"]),
            ("[::1]:50051", vec!["[::1]:50051"]),
            ("10.0.0.4:50051", vec!["10.0.0.4:50051"]),
            ("balanced.local:443", vec!["10.0.0.5:443"]),
        ]);
        let fleet = discover_workers_with(&endpoints, resolve).await.unwrap();
        assert_eq!(fleet.len(), 5);
        for url in &fleet {
            let keyed = Url::parse(url).unwrap_or_else(|err| panic!("emitted `{url}`: {err}"));
            assert!(
                keyed.host_str().is_some(),
                "emitted `{url}` has no origin, so a worker would be keyed on the whole string"
            );
            // `port_or_known_default`, not `port`: an emitted `https://10.0.0.5:443` writes its
            // port out and `Url` erases it again on the way back in — the same erasure
            // [`Endpoint::written_default_port`] exists for, seen from the other end.
            assert!(
                keyed.port_or_known_default().is_some(),
                "emitted `{url}` must carry a port"
            );
        }
    }

    /// The converse property, on the shape #109's review caught: an endpoint written without its
    /// scheme parses into a scheme with a path and *no host*. Which of `parse`'s checks refuses it
    /// is an implementation detail; that discovery refuses it here — rather than resolving it into
    /// the fleet list, where nothing downstream can key it — is not.
    #[test]
    fn an_endpoint_with_no_origin_is_refused_here() {
        for raw in ["w1:50051", "worker.local:50051", "mailto:ops@example.com"] {
            assert_eq!(
                Url::parse(raw).unwrap().host_str(),
                None,
                "`{raw}` is expected to parse without a host"
            );
            assert!(
                Endpoint::parse(raw).is_err(),
                "`{raw}` has no origin, so discovery must refuse it rather than resolve it"
            );
        }
    }

    /// #124. A worker URL has no legitimate userinfo, so it is refused rather than ignored — and
    /// the refusal names the endpoint without naming the credential.
    #[test]
    fn an_endpoint_carrying_a_credential_is_refused() {
        let err = Endpoint::parse("http://ops:hunter2@w1:50051")
            .err()
            .expect("an endpoint carrying a credential must be refused");
        let rendered = format!("{err:#}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        // Still diagnosable: the host and the user survive, which is what tells an operator which
        // `--workers` entry to edit.
        assert!(rendered.contains("w1:50051"), "{rendered}");
        assert!(rendered.contains("ops"), "{rendered}");
        assert!(rendered.contains("LLDB_FLEET_TOKEN"), "{rendered}");
    }

    /// The property the refusal alone does not deliver, and the reason `parse` redacts every
    /// message rather than only that one: a credential must not survive into a message **whichever
    /// way the endpoint fails**, including the ways `Url` does not recognize as credentials at all.
    ///
    /// One row per message arm in `Endpoint::parse`. The second is the shape #124 is actually
    /// about — with no scheme it parses as scheme `ops` and an opaque path, so `url.password()` is
    /// `None` and the refusal above never fires while the password sits in the text.
    #[test]
    fn a_credential_reaches_no_message_however_the_endpoint_fails() {
        let leaky = [
            "http://ops:hunter2@w1:50051", // the userinfo refusal
            "ops:hunter2@w1:50051",        // parses as a scheme with no host
            "http://ops:hunter2@w1",       // no port written
            "ftp://ops:hunter2@w1:50051",  // unsupported scheme
            "http://ops:hunter2@[bad",     // not a URL at all
        ];
        for raw in leaky {
            let err = Endpoint::parse(raw)
                .err()
                .unwrap_or_else(|| panic!("`{raw}` must be refused"));
            let rendered = format!("{err:#}");
            assert!(!rendered.contains("hunter2"), "`{raw}` leaked: {rendered}");
            assert!(
                rendered.contains(REDACTED),
                "`{raw}` unredacted: {rendered}"
            );
        }
    }

    /// The contrast that justifies a second redactor existing at all: `services::redact_url` drops
    /// an unparseable string whole, which is right for a connection URL and wrong here, because the
    /// commonest leaky `--workers` value is exactly the one `Url` cannot parse.
    #[test]
    fn redact_endpoint_keeps_the_host_where_redact_url_drops_it() {
        let raw = "ops:hunter2@w1:50051";
        let redacted = redact_endpoint(raw);
        assert_eq!(redacted, "ops:****@w1:50051");
        let dropped = crate::services::redact_url(raw);
        assert!(
            !dropped.contains("w1"),
            "expected the host dropped: {dropped}"
        );
        assert!(!dropped.contains("hunter2"), "{dropped}");
    }

    /// No credential means no rewrite and no allocation, and a bare `user@host` counts as no
    /// credential — blanking it would invent a password nobody wrote.
    #[test]
    fn redact_endpoint_leaves_an_endpoint_with_nothing_to_hide_alone() {
        for raw in [
            "http://w1:50051",
            "https://[::1]:50051",
            "ops@w1:50051",
            "http://w1:50051/a@b",
            "",
        ] {
            assert!(
                matches!(redact_endpoint(raw), Cow::Borrowed(same) if same == raw),
                "`{raw}` was rewritten"
            );
        }
    }
}
