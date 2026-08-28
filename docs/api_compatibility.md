# API Compatibility

Compatibility surfaces:

- native length-prefixed frames in `neo4r-protocol`
- TCP line protocol commands in `neo4r-server::protocol`
- HTTP JSON endpoints in the web console and admin API
- Rust SDK request/response decoding
- Python SDK request/response decoding

Rules:

- new response fields must be additive for JSON APIs.
- line protocol command names and field order need tests before changing.
- native frame magic, request id correlation, and typed row encoding are golden
  compatibility surfaces.
- SDK examples must run against a live server before a release.

CI entrypoint:

```bash
scripts/sdk-compat.sh
NEO4R_RUN_SDK_COMPAT=1 scripts/sdk-compat.sh
```
