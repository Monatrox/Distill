//! Enforces destination policy for outbound HTTP sinks.
//!
//! HTTP sinks are configured through channel definitions or sink manifests, so
//! their destinations must be validated against an operator-controlled policy
//! before a client request is made from Distill's network identity.

// Rust guideline compliant 2026-02-21

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};

/// Sink ports admitted without an explicit allowlist entry.
///
/// These are the standard HTTP(S) serving ports; any other port must be
/// operator-approved through `DISTILL_SINK_ALLOWLIST`.
const DEFAULT_HTTP_SINK_PORTS: [u16; 4] = [80, 443, 8080, 8443];

/// Private and reserved IPv4 ranges that outbound sinks must not reach.
const FORBIDDEN_V4_RANGES: [(Ipv4Addr, u8); 14] = [
    (Ipv4Addr::UNSPECIFIED, 8),           // this network
    (Ipv4Addr::new(10, 0, 0, 0), 8),      // private
    (Ipv4Addr::new(100, 64, 0, 0), 10),   // carrier-grade NAT
    (Ipv4Addr::new(127, 0, 0, 0), 8),     // loopback
    (Ipv4Addr::new(169, 254, 0, 0), 16),  // link-local
    (Ipv4Addr::new(172, 16, 0, 0), 12),   // private
    (Ipv4Addr::new(192, 0, 0, 0), 24),    // IETF protocol assignments
    (Ipv4Addr::new(192, 0, 2, 0), 24),    // TEST-NET-1
    (Ipv4Addr::new(192, 168, 0, 0), 16),  // private
    (Ipv4Addr::new(198, 18, 0, 0), 15),   // benchmarking
    (Ipv4Addr::new(198, 51, 100, 0), 24), // TEST-NET-2
    (Ipv4Addr::new(203, 0, 113, 0), 24),  // TEST-NET-3
    (Ipv4Addr::new(224, 0, 0, 0), 4),     // multicast
    (Ipv4Addr::new(240, 0, 0, 0), 4),     // reserved
];

/// IPv6 ranges that outbound sinks must not reach.
const FORBIDDEN_V6_RANGES: [(Ipv6Addr, u8); 9] = [
    (Ipv6Addr::UNSPECIFIED, 128),
    (Ipv6Addr::LOCALHOST, 128),
    (Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0), 96), // NAT64
    (Ipv6Addr::new(0x0100, 0, 0, 0, 0, 0, 0, 0), 64),    // discard-only
    (Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32), // documentation
    (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),     // unique local
    (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),    // link-local
    (Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10),    // site-local
    (Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8),     // multicast
];

/// Operator-approved sink destinations.
///
/// An empty allowlist permits only public `http`/`https` destinations on the
/// default ports. Entries are `host[:port]`, `cidr[:port]`, or `:port`.
#[derive(Debug, Clone, Default)]
pub struct SinkAllowlist {
    hosts: Vec<(String, Option<u16>)>,
    cidrs: Vec<(IpAddr, u8, Option<u16>)>,
    ports: Vec<u16>,
}

impl SinkAllowlist {
    /// Parses a comma-separated allowlist value.
    ///
    /// # Errors
    /// Returns an error when an entry is not a valid hostname, CIDR, or port.
    pub fn parse(value: &str) -> Result<Self, String> {
        let mut allowlist = Self::default();
        for entry in value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let (spec, port) = match split_port(entry) {
                Some((spec, port)) => (spec, Some(port)),
                None => (entry, None),
            };
            if spec.is_empty() {
                let Some(port) = port else {
                    return Err(format!("invalid allowlist entry '{entry}'"));
                };
                allowlist.ports.push(port);
                continue;
            }
            if spec.contains('/') {
                let (network, prefix) = parse_cidr(spec)
                    .map_err(|error| format!("invalid allowlist entry '{entry}': {error}"))?;
                allowlist.cidrs.push((network, prefix, port));
            } else if spec.parse::<IpAddr>().is_ok() {
                return Err(format!(
                    "invalid allowlist entry '{entry}': raw IP addresses must use CIDR notation"
                ));
            } else if !is_valid_hostname(spec) {
                return Err(format!("invalid allowlist entry '{entry}'"));
            } else {
                allowlist.hosts.push((spec.to_ascii_lowercase(), port));
            }
        }
        Ok(allowlist)
    }

    fn allows_host(&self, host: &str, port: u16) -> bool {
        self.hosts
            .iter()
            .any(|(allowed, rule_port)| allowed == host && self.matches_port(*rule_port, port))
    }

    fn allows_ip(&self, ip: IpAddr, port: u16) -> bool {
        self.cidrs.iter().any(|(network, prefix, rule_port)| {
            ip_in_cidr(ip, *network, *prefix) && self.matches_port(*rule_port, port)
        })
    }

    fn matches_port(&self, rule_port: Option<u16>, port: u16) -> bool {
        rule_port.map_or_else(
            || DEFAULT_HTTP_SINK_PORTS.contains(&port) || self.ports.contains(&port),
            |allowed| allowed == port,
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct PolicyResolver {
    allowlist: SinkAllowlist,
    port: u16,
}

impl reqwest::dns::Resolve for PolicyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let policy = self.clone();
        Box::pin(async move {
            let host = name.as_str();
            let addresses: Vec<_> = tokio::net::lookup_host((host, policy.port))
                .await?
                .collect();
            if addresses.is_empty()
                || (!policy.allowlist.allows_host(host, policy.port)
                    && addresses.iter().any(|address| {
                        is_forbidden_ip(address.ip())
                            && !policy.allowlist.allows_ip(address.ip(), policy.port)
                    }))
            {
                return Err(std::io::Error::other(
                    "HTTP sink DNS resolved to a prohibited destination",
                )
                .into());
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Validates an HTTP sink destination against the outbound policy.
///
/// Enforces the URL scheme, the destination port, and the resolved addresses
/// of the host. Raw IP addresses are only admitted through an allowlisted
/// CIDR; hostnames must resolve only to public addresses unless the hostname
/// itself is allowlisted.
///
/// # Errors
/// Returns an error when the endpoint violates the policy.
#[cfg(test)]
fn validate_sink_url(endpoint: &str, allowlist: &SinkAllowlist) -> Result<(), String> {
    resolve_sink_url(endpoint, allowlist).map(|_| ())
}

pub(super) fn resolve_sink_url(
    endpoint: &str,
    allowlist: &SinkAllowlist,
) -> Result<PolicyResolver, String> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| "invalid HTTP sink URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("HTTP sink URL scheme must be http or https".to_string());
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let host = url
        .host_str()
        .ok_or("HTTP sink URL must include a host")?
        .trim_matches(['[', ']'])
        .to_ascii_lowercase();
    let resolver = PolicyResolver {
        allowlist: allowlist.clone(),
        port,
    };
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if allowlist.allows_ip(ip, port) {
            Ok(resolver)
        } else {
            Err("raw addresses require an allowlisted CIDR and port".to_string())
        };
    }
    if allowlist.allows_host(&host, port) {
        return Ok(resolver);
    }
    if !DEFAULT_HTTP_SINK_PORTS.contains(&port) && !allowlist.ports.contains(&port) {
        return Err("HTTP sink destination port is not allowed".to_string());
    }
    let addresses: Vec<_> = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|_| "HTTP sink host could not be resolved".to_string())?
        .collect();
    if addresses.is_empty()
        || addresses.iter().any(|address| {
            is_forbidden_ip(address.ip()) && !allowlist.allows_ip(address.ip(), port)
        })
    {
        return Err("HTTP sink resolves to a destination that is not allowed".to_string());
    }
    Ok(resolver)
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => FORBIDDEN_V4_RANGES
            .iter()
            .any(|(network, prefix)| ip_in_cidr(ip.into(), (*network).into(), *prefix)),
        IpAddr::V6(ip) => FORBIDDEN_V6_RANGES
            .iter()
            .any(|(network, prefix)| ip_in_cidr(ip.into(), (*network).into(), *prefix)),
    }
}

/// Reports whether `ip` falls inside `network` with the given prefix length.
fn ip_in_cidr(ip: IpAddr, network: IpAddr, prefix: u8) -> bool {
    match (ip, network) {
        (IpAddr::V4(ip), IpAddr::V4(network)) => {
            let prefix = prefix.min(32);
            if prefix == 0 {
                return true;
            }
            let mask = u32::MAX << (32 - prefix);
            u32::from(ip) & mask == u32::from(network) & mask
        }
        (IpAddr::V6(ip), IpAddr::V6(network)) => {
            let prefix = prefix.min(128);
            if prefix == 0 {
                return true;
            }
            let mask = u128::MAX << (128 - prefix);
            u128::from(ip) & mask == u128::from(network) & mask
        }
        _ => false,
    }
}

fn parse_cidr(value: &str) -> Result<(IpAddr, u8), String> {
    let (address, prefix) = value
        .split_once('/')
        .ok_or_else(|| "CIDR must contain '/'".to_string())?;
    let ip: IpAddr = address
        .parse()
        .map_err(|_| format!("invalid IP address '{address}'"))?;
    let maximum = match ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    let prefix: u8 = prefix
        .parse()
        .map_err(|_| "prefix length must be an integer".to_string())?;
    if prefix > maximum {
        return Err(format!("prefix {prefix} exceeds {maximum} for '{address}'"));
    }
    Ok((ip, prefix))
}

fn is_valid_hostname(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '.' | '_')
        })
}

/// Splits a trailing `:port` suffix when the remainder after the last colon is
/// all digits. IPv6 addresses contain colons but never end with a bare number.
fn split_port(entry: &str) -> Option<(&str, u16)> {
    let (spec, suffix) = entry.rsplit_once(':')?;
    let port = suffix.parse::<u16>().ok()?;
    Some((spec, port))
}

#[cfg(test)]
mod tests {
    use super::{SinkAllowlist, ip_in_cidr, is_forbidden_ip, split_port, validate_sink_url};
    use std::net::IpAddr;

    fn allowlist(value: &str) -> SinkAllowlist {
        SinkAllowlist::parse(value).expect("test allowlist should parse")
    }

    fn validate(endpoint: &str, value: &str) -> Result<(), String> {
        validate_sink_url(endpoint, &allowlist(value))
    }

    #[test]
    fn default_policy_rejects_non_http_schemes() {
        assert!(validate("ftp://example.com/logs", "").is_err());
        assert!(validate("file:///etc/passwd", "").is_err());
        assert!(validate("http://example.com/logs", "example.com").is_ok());
        assert!(validate("https://example.com/logs", "example.com").is_ok());
    }

    #[test]
    fn default_policy_rejects_raw_ip_addresses() {
        assert!(validate("http://127.0.0.1/logs", "").is_err());
        assert!(validate("http://[::1]/logs", "").is_err());
        assert!(validate("http://10.0.0.5/logs", "").is_err());
    }

    #[test]
    fn cidr_allowlist_admits_raw_addresses() {
        assert!(validate("http://10.20.5.5:9090/logs", "10.20.0.0/16:9090").is_ok());
        assert!(validate("http://10.30.5.5:9090/logs", "10.20.0.0/16:9090").is_err());
    }

    #[test]
    fn default_policy_rejects_loopback_hostnames() {
        assert!(validate("http://localhost/logs", "").is_err());
    }

    #[test]
    fn allowlisted_hostname_bypasses_resolution() {
        assert!(validate("http://internal-api:9090/logs", "internal-api:9090").is_ok());
        assert!(validate("http://internal-api/logs", "internal-api").is_ok());
    }

    #[test]
    fn default_policy_rejects_non_default_ports() {
        assert!(validate("http://example.com:9090/logs", "").is_err());
        assert!(validate("http://example.com:443/logs", "example.com").is_ok());
        assert!(validate("https://example.com/logs", "example.com").is_ok());
    }

    #[test]
    fn allowlisted_port_admits_non_default_ports() {
        assert!(validate("http://example.com:9090/logs", "example.com,:9090").is_ok());
        assert!(validate("http://example.com:9091/logs", "example.com,:9090").is_err());
    }

    #[test]
    fn allowlist_parsing_rejects_malformed_entries() {
        assert!(SinkAllowlist::parse("10.20.0.0/33").is_err());
        assert!(SinkAllowlist::parse("2001:db8::/129").is_err());
        assert!(SinkAllowlist::parse("1.2.3.4").is_err());
        assert!(SinkAllowlist::parse("host:notaport").is_err());
    }

    #[test]
    fn allowlist_parsing_accepts_ipv6_cidrs() {
        let allowlist = SinkAllowlist::parse("2001:db8::/32,fe80::/10:8080").unwrap();
        assert!(allowlist.allows_ip("2001:db8::1".parse().unwrap(), 80));
        assert!(!allowlist.allows_ip("2001:db9::1".parse().unwrap(), 80));
    }

    #[test]
    fn host_and_cidr_ports_do_not_cross_authorize_each_other() {
        let allowlist = SinkAllowlist::parse("first:9090,second:9091,10.20.0.0/16:9092").unwrap();
        assert!(allowlist.allows_host("first", 9090));
        assert!(!allowlist.allows_host("first", 9091));
        assert!(!allowlist.allows_host("second", 9092));
        assert!(!allowlist.allows_ip("10.20.1.1".parse().unwrap(), 9090));
        assert!(allowlist.allows_ip("10.20.1.1".parse().unwrap(), 9092));
    }

    #[test]
    fn cidr_membership_matches_network_bounds() {
        assert!(ip_in_cidr(
            "10.20.5.5".parse().unwrap(),
            "10.20.0.0".parse().unwrap(),
            16
        ));
        assert!(!ip_in_cidr(
            "10.21.5.5".parse().unwrap(),
            "10.20.0.0".parse().unwrap(),
            16
        ));
        assert!(ip_in_cidr(
            "192.168.1.1".parse().unwrap(),
            "192.168.1.1".parse().unwrap(),
            32
        ));
    }

    #[test]
    fn forbidden_ranges_cover_loopback_and_private_space() {
        assert!(is_forbidden_ip("127.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(is_forbidden_ip("::1".parse::<IpAddr>().unwrap()));
        assert!(is_forbidden_ip("10.1.2.3".parse::<IpAddr>().unwrap()));
        assert!(is_forbidden_ip(
            "169.254.169.254".parse::<IpAddr>().unwrap()
        ));
        assert!(is_forbidden_ip("fc00::1".parse::<IpAddr>().unwrap()));
        assert!(!is_forbidden_ip("8.8.8.8".parse::<IpAddr>().unwrap()));
        assert!(!is_forbidden_ip("2606:4700::1".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn port_splitting_handles_ipv6_and_ports() {
        assert_eq!(split_port("host:9090"), Some(("host", 9090)));
        assert_eq!(split_port("2001:db8::/32"), None);
        assert_eq!(
            split_port("2001:db8::/32:9090"),
            Some(("2001:db8::/32", 9090))
        );
        assert_eq!(split_port("::1/128"), None);
    }
}
