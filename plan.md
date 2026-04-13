# galay-etcd Feature Plan

## Core Client

- [x] Sync client: `EtcdClient`
- [x] Async client: `AsyncEtcdClient`
- [x] Endpoint parsing and API prefix normalization
- [x] Basic error mapping and last-result cache

## KV Operations

- [x] Put
- [x] Get
- [x] Delete
- [x] Prefix query
- [x] Prefix delete
- [x] Lease grant
- [x] Lease keepalive once

## Transaction / Batch

- [x] Pipeline success-ops wrapper over `POST /v3/kv/txn`
- [ ] Compare/failure transaction DSL
- [ ] Compare-and-swap / conditional update helpers

## Async / Runtime

- [x] Async connect / close awaitables
- [x] Async put/get/delete/lease/pipeline awaitables
- [x] Async benchmark support
- [x] Async HTTP serialized request fast path via `galay-http`

## Build / Package / Examples

- [x] CMake package export
- [x] `BUILD_TESTING` compatible test switch
- [x] Include examples
- [x] Import/module examples on supported toolchains
- [x] Public tests `T1` to `T8`
- [x] Public benchmarks `B1` and `B2`
- [x] Rust benchmark comparison harness

## Compatibility / Limits

- [x] Explicitly reject unsupported `https://` endpoints
- [x] Explicitly document unsupported auth/watch/lock/member APIs
- [ ] TLS / HTTPS transport
- [ ] Username/password or token auth
- [ ] Watch streaming API
- [ ] Official lock / election helpers
- [ ] Cluster / member management API

## Release Follow-up

- [ ] Add TLS support if target environment requires secure etcd access
- [ ] Add auth support if target environment enables etcd auth
- [ ] Add conditional transaction API before advertising distributed lock scenarios
