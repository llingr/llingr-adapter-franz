# llingr-adapter-franz

[![CI](https://github.com/llingr/llingr-adapter-franz/actions/workflows/ci.yml/badge.svg)](https://github.com/llingr/llingr-adapter-franz/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/llingr/llingr-adapter-franz.svg)](https://pkg.go.dev/github.com/llingr/llingr-adapter-franz)
[![Go Report Card](https://goreportcard.com/badge/github.com/llingr/llingr-adapter-franz)](https://goreportcard.com/report/github.com/llingr/llingr-adapter-franz)
[![Tag](https://img.shields.io/github/v/tag/llingr/llingr-adapter-franz)](https://github.com/llingr/llingr-adapter-franz/tags)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/llingr/llingr-adapter-franz)](go.mod)

Pure Go Kafka adapter for the llingr message processing ecosystem using
[franz-go](https://github.com/twmb/franz-go).

## Overview

This adapter wraps `twmb/franz-go` to implement `nexus.BrokerPort[*kgo.Record]`
from [llingr-nexus](https://github.com/llingr/llingr-nexus). This adapter is **pure Go** with no C dependencies.

Each adapter instance consumes from a single topic. For multiple topics, create
multiple consumers.

## Why franz-go?

| Feature           | franz-go       | confluent-kafka-go  |
|-------------------|----------------|---------------------|
| Implementation    | Pure Go        | CGO (librdkafka)    |
| Producing speed   | ~4x faster     | Baseline            |
| Consuming speed   | ~10-20x faster | Baseline            |
| Cross-compilation | Easy           | Complex             |
| Static binaries   | Supported      | Not supported       |

## Broker Compatibility

franz-go works with any Kafka-compatible broker: Apache Kafka (0.8.0+), Redpanda,
Amazon MSK, Confluent Platform, and Azure Event Hubs (Kafka protocol mode).

## Installation

```bash
go get github.com/llingr/llingr-adapter-franz
```

## Quick Start

See [API Summary](#api-summary) for more options.

```go
var builder nexus.ConsumerBuilder[*kgo.Record] // init builder impl

adapter := franzadapter.New(ctx, "my-group",
    "broker-1:9092", "broker-2:9092", "broker-3:9092")

consumer, err := adapter.CreateConsumer(builder)
if err != nil {
    log.Fatal(err)
}

defer consumer.Shutdown()
consumer.Subscribe()
```

## Authentication

Authentication method depends on your broker configuration - check with your platform
team if unsure.

| Auth Method         | Requires                                     | Section            |
|---------------------|----------------------------------------------|--------------------|
| Username/password   | username, password, CA cert                  | SASL/SCRAM         |
| Client certificates | CA cert, client cert, client key             | mTLS               |
| OAuth/OIDC          | token endpoint URL, client ID, client secret | OAUTHBEARER        |
| AWS MSK             | AWS credentials or IAM role                  | AWS MSK IAM        |
| Confluent Cloud     | API key, API secret                          | SASL/SCRAM (PLAIN) |

Use `NewWithOptions()` for SASL, TLS, or other custom `kgo.Opt` settings.

### SASL/SCRAM

Username/password authentication with secure hashing. Use `AsSha256Mechanism()`
or `AsSha512Mechanism()` depending on your broker configuration.

```go
import (
    "crypto/tls"
    "crypto/x509"
    "os"

    "github.com/twmb/franz-go/pkg/sasl/scram"
)

// Load CA cert (get this from your platform team)
caCert, _ := os.ReadFile("/path/to/ca.crt")
caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

adapter, err := franzadapter.NewWithOptions(ctx, "my-group",
    []string{"broker:9093"},
    kgo.SASL(scram.Auth{
        User: "user",
        Pass: "pass",
    }.AsSha256Mechanism()),
    kgo.DialTLSConfig(&tls.Config{
        RootCAs: caCertPool,
    }),
)
```

### TLS

Encrypt traffic and verify the broker's certificate (no SASL).

```go
import "crypto/tls"

caCert, _ := os.ReadFile("/path/to/ca.crt")
caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

adapter, err := franzadapter.NewWithOptions(ctx, "my-group",
    []string{"broker:9093"},
    kgo.DialTLSConfig(&tls.Config{
        RootCAs: caCertPool,
    }),
)
```

### mTLS (Client Certificates)

Mutual TLS where the broker also verifies the client's identity.

```go
import "crypto/tls"

// Load CA cert
caCert, _ := os.ReadFile("/path/to/ca.crt")
caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

// Load client cert and key
clientCert, _ := tls.LoadX509KeyPair("/path/to/client.crt", "/path/to/client.key")

adapter, err := franzadapter.NewWithOptions(ctx, "my-group",
    []string{"broker:9093"},
    kgo.DialTLSConfig(&tls.Config{
        RootCAs:      caCertPool,
        Certificates: []tls.Certificate{clientCert},
    }),
)
```

### OAUTHBEARER

JWT-based authentication for identity providers like Keycloak, Okta, or Confluent Cloud.
The provider function is called on each authentication attempt (initial connect and re-auth),
so it should fetch a fresh token each time.

```go
import (
    "encoding/json"
    "net/http"
    "net/url"
    "strings"

    "github.com/twmb/franz-go/pkg/sasl/oauth"
)

// Token provider - called on each authentication attempt
tokenProvider := oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
    // Client credentials flow (common for service-to-service)
    data := url.Values{}
    data.Set("grant_type", "client_credentials")
    data.Set("client_id", "my-service")
    data.Set("client_secret", os.Getenv("OAUTH_CLIENT_SECRET"))

    // Keycloak example - Okta, Azure AD, Auth0 etc. have different endpoint URLs
    req, _ := http.NewRequestWithContext(ctx, "POST",
        "https://keycloak.example.com/realms/kafka/protocol/openid-connect/token",
        strings.NewReader(data.Encode()))
    req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return oauth.Auth{}, err
    }
    defer resp.Body.Close()

    var result struct {
        AccessToken string `json:"access_token"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return oauth.Auth{}, err
    }
    return oauth.Auth{Token: result.AccessToken}, nil
})

adapter, err := franzadapter.NewWithOptions(ctx, "my-group",
    []string{"broker:9094"},
    kgo.SASL(tokenProvider),
)
```

Add `kgo.DialTLSConfig(tlsConfig)` if your broker requires TLS (SASL_SSL vs SASL_PLAINTEXT).

### AWS MSK IAM

For Amazon MSK with IAM authentication. MSK requires TLS.

```go
import (
    "crypto/tls"

    "github.com/twmb/franz-go/pkg/sasl/aws"
)

adapter, err := franzadapter.NewWithOptions(ctx, "my-group",
    []string{"broker:9093"},
    kgo.SASL(aws.Auth{
        AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
    }.AsManagedStreamingIAMMechanism()),
    kgo.DialTLSConfig(&tls.Config{}), // empty config uses system CA pool
)
```

### Supported Mechanisms

| Mechanism         | Package         | Use Case                            |
|-------------------|-----------------|-------------------------------------|
| SCRAM-SHA-256/512 | `sasl/scram`    | Secure password authentication      |
| PLAIN             | `sasl/plain`    | Simple username/password            |
| OAUTHBEARER       | `sasl/oauth`    | Bearer tokens (Confluent, Keycloak) |
| GSSAPI            | `sasl/kerberos` | Kerberos/Active Directory           |
| AWS MSK IAM       | `sasl/aws`      | Amazon MSK native auth              |

## API Summary

| Function                                       | Use Case                   |
|------------------------------------------------|----------------------------|
| `New(ctx, group, brokers...)`                  | Simple setup, no auth      |
| `NewWithOptions(ctx, group, brokers, opts...)` | SASL, TLS, custom settings |
| `NewCustom()` + `SetClient()`                  | Full kgo.Client control    |

### Options Behaviour

When using `New()` or `NewWithOptions()`:
- **CAN override**: Balancers, fetch settings, timeouts, SASL, TLS
- **CANNOT override**: `DisableAutoCommit`, `BlockRebalanceOnPoll`, rebalance callbacks

These restrictions ensure correct operation with nexus consumers.

## Advanced Configuration

### Full Client Control (NewCustom)

For complete control over `kgo.Client` configuration, use the two-phase setup:

```go
// 1. Create adapter first (without client)
adapter := franzadapter.NewCustom()

// 2. Create kgo.Client with adapter's rebalance callbacks
client, err := kgo.NewClient(
    kgo.SeedBrokers("broker:9093"),
    kgo.ConsumerGroup("my-group"),
    kgo.ConsumeTopics("orders"),
    // Required for nexus adapters:
    kgo.DisableAutoCommit(),
    kgo.BlockRebalanceOnPoll(),
    kgo.OnPartitionsAssigned(adapter.OnAssigned),
    kgo.OnPartitionsRevoked(adapter.OnRevoked),
    kgo.OnPartitionsLost(adapter.OnLost),
    // Recommended:
    kgo.Balancers(kgo.CooperativeStickyBalancer()),
    // Your options:
    kgo.SASL(scram.Auth{User: "user", Pass: "pass"}.AsSha256Mechanism()),
    kgo.DialTLSConfig(tlsConfig),
)
if err != nil {
    log.Fatal(err)
}

// 3. Wire up
adapter.SetClient(client)
consumer := adapter.CreateConsumer(builder)
```

### Rebalance Handling

The adapter uses cooperative sticky balancing with `BlockRebalanceOnPoll` to
coordinate rebalances with the poll loop. This is handled transparently - you
don't need to configure anything unless using `NewCustom()`.

### Consumer Group Compatibility

By default the adapter uses `CooperativeStickyBalancer`, which provides the most
efficient rebalancing behaviour by only revoking partitions that need to move rather
than stopping all processing during membership changes. This is the recommended
configuration for new deployments where all consumers use this adapter.

When joining an existing consumer group that includes legacy consumers using different
rebalance protocols (such as Java consumers configured with `range` or `roundrobin`
assignors), the Kafka group coordinator requires all members to agree on a common
protocol. If the existing members don't support cooperative rebalancing, the new
consumer will fail to join the group.

For these mixed-deployment scenarios, the adapter provides `CompatibilityBalancers()`
as an opt-in option that configures multiple fallback protocols in priority order:
cooperative-sticky, sticky, roundrobin, and finally range. During group join, Kafka
selects the highest-priority protocol that all members support. If the preferred
cooperative protocol isn't available, the adapter logs a warning explaining which
protocols were unavailable and which fallback was selected.

```go
adapter := franzadapter.NewWithOptions(ctx, "my-group",
    []string{"broker:9092"},
    franzadapter.CompatibilityBalancers(),
)
```

This approach is particularly useful during rolling migrations where you're gradually
replacing legacy consumers with ones using this adapter - the group continues operating
with the legacy protocol until all members support cooperative rebalancing, at which
point Kafka automatically upgrades to the more efficient protocol. Once migration is
complete and all consumers support cooperative rebalancing, you can remove the
`CompatibilityBalancers()` option to ensure the optimal protocol is always used and
any misconfiguration fails explicitly rather than silently falling back.

## Bandwidth Telemetry

The adapter implements `nexus.BandwidthPort` for wire-level bandwidth metering.
Enable it by calling `WithBandwidthInterval` before `CreateConsumer`:

```go
adapter := franzadapter.New(ctx, "my-group", "broker:9092")
adapter.WithBandwidthInterval(time.Minute) // collection cadence: 1s to 12h
consumer, _ := adapter.CreateConsumer(builder)
```

The adapter emits per-interval deltas by resetting internal accumulators after
each collection. Byte counts reflect decompressed payload sizes as reported by
franz-go's fetch batch hooks. Compression visibility is fully supported:
CompressedBytes, UncompressedBytes, and the compression algorithm name are
populated on every packet.

Byte counts measure at the application payload layer rather than the wire
protocol layer. This typically results in values that are fractionally lower
(under 1%) than wire-level counts from the same traffic, since Kafka protocol
framing and record batch headers are not included. Both measurements are
legitimate for bandwidth-based metering - they simply measure at different
layers of the stack.

## Repository Layout

This repo is split into two Go modules:

```
llingr-adapter-franz/
├── go.mod                     # franzadapter module - what consumers import
├── franzadapter/              # public API + unit tests
└── integration/
    ├── go.mod                 # separate module: testcontainers + Docker deps
    ├── simple_consumer_helper_test.go
    ├── simple_consumer_test.go
    └── franz_integration_test.go
```

The integration tests live in their own module so the large testcontainers/Docker
dependency tree (around 40 transitive packages) stays out of `go.mod`. Anyone
running `go get github.com/llingr/llingr-adapter-franz` pulls only the two
direct deps the runtime actually needs: `franz-go` and `llingr-nexus`.

The `integration` module declares
`replace github.com/llingr/llingr-adapter-franz => ../`, so its tests always
exercise the local source tree rather than a published version. The
`integration` module is never published.

## Development

```bash
# Unit tests only (fast, no Docker, franzadapter module)
make test

# Integration tests (requires Docker, integration module)
make integration

# Build, unit tests, and integration tests (the default target)
make
```

The franzadapter module's minimum Go version is 1.25 (inherited from
`franz-go` v1.21+). The integration module requires Go 1.26.3, so contributors
running `make integration` need a 1.26.3 toolchain installed. Consumers of the
published `franzadapter` package are unaffected.

CI runs each module as a separate job; the integration job has Docker
available on the runner.

## Licence

Apache-2.0 - see [LICENSE](./LICENSE) and [COPYRIGHT](./COPYRIGHT).
Contributions are governed by [CONTRIBUTING.md](./CONTRIBUTING.md).
