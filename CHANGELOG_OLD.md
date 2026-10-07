# Older changes
## 1.6.0
* (proxy) Timestamps are no longer falsified by copying: source `ts`/`lc`/`q` are passed through to the mirror
* (proxy) Start, cycle and force sync skip inactive sources (configurable staleness limit, quality check)
* (proxy) Force sync only writes values that actually differ; cycle sync respects the ack filter
* (proxy) New remote sync placeholders `%LC%`, `%ACK%`, `%Q%` and option to skip inactive sources

## 1.5.1
* (proxy) Dual mode accepts commands on the `/set` topic regardless of the ack flag (MQTT adapters forward
  `/set` as unconfirmed command); clarified that the topic mode only affects the MQTT side

## 1.5.0
* (proxy) Per-mapping topic mode: `dual` sends the state on `<topic>` and receives commands on `<topic>/set`
  (suffix configurable); stale commands are no longer replayed on start or force sync

## 1.4.0
* (proxy) Fixed echo protection swallowing genuine device confirmations on `both` mappings
* (proxy) HTTPS option for the dashboard, persistent login lockout, retryable-error handling
* (proxy) Type coercion for the command direction, tolerant value comparison

## 1.3.4
* (proxy) Remote sync: reconstructs missing line breaks around BEGIN/END markers in pasted CA certificates
  (fixes single-line PEM "no start line" errors)

## 1.3.3
* (proxy) Remote sync: normalises look-alike Unicode dashes/non-breaking spaces in pasted CA certificates

## 1.3.2
* (proxy) Remote sync: logs the fingerprint of the parsed CA certificate for diagnostics, disables proxy
  environment variables for the sync request

## 1.3.1
* (proxy) Visible `info.version` state and diagnostic logging for the remote sync CA certificate

## 1.3.0
* (proxy) Security hardening (dashboard authentication, CSRF/CORS protection, TLS pinning), deterministic
  echo protection, batched sync, configurable ack filter

## 1.2.2
* (proxy) Force sync interval to heal out-of-sync states automatically

## 1.2.1
* (proxy) Timestamp validation to prevent hardware echoes

## 1.2.0
* (proxy) Performance update with lookup maps for instant reaction
