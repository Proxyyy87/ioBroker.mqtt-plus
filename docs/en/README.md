# ioBroker.mqtt-plus

![Logo](../../admin/mqtt-plus.png)

**License:** MIT · [Deutsche Version](../de/README.md)

## Summary

`mqtt-plus` is an ioBroker adapter that acts as a bridge between ioBroker states and an MQTT
broker (more precisely: between ioBroker states and the namespace of an already installed MQTT
client adapter, e.g. `mqtt.0`). It automatically creates the required folder structure on the
target side, converts values on the fly, provides a secured web dashboard for backup/restore and
structure analysis, and can additionally push data actively to an external webhook (remote sync).

`mqtt-plus` does not speak the MQTT protocol itself — connection, QoS and `retain` towards the
actual broker are handled entirely by the separately installed MQTT adapter, whose namespace
`mqtt-plus` only reads from and writes to.

## Main features

* **Bidirectional mirroring:** Synchronises values from ioBroker to MQTT (`out`), from MQTT to
  ioBroker (`in`) or in both directions (`both`).
* **Topic mode per mapping:** `single` (one shared topic) or `dual` following the common MQTT
  convention — state on `<topic>`, commands on `<topic>/set` (suffix configurable).
* **Automatic structure creation:** Recursively creates missing folders and states on the target
  side based on the MQTT paths (e.g. `home/kitchen/light` becomes `home.kitchen.light`).
* **Type conversion:** Rounds numbers (with configurable decimals), converts boolean ↔ number, or
  with `Auto` detects the actual ioBroker data type of the target object — in both directions.
* **Deterministic echo protection:** Prevents feedback loops on `both` mappings without
  swallowing real device confirmations (see [Echo protection](#echo-protection-for-both-mappings)).
* **Ack filter per mapping:** States without a real device behind them (`0_userdata.0.*`,
  `alias.0.*`) can be synchronised immediately instead of with a delay.
* **Force sync:** An additional, configurable interval that bypasses the cache, compares source
  and target directly and only rewrites values that actually differ — heals states that drifted
  apart after a restart or a lost connection.
* **Unaltered timestamps:** The mirror takes over `ts` (last update), `lc` (last change) and `q`
  (quality) of the source. Inactive devices do not become "fresh" again on the target side
  (see [Timestamps & staleness](#timestamps--staleness)).
* **Secured web dashboard:** Login required (basic auth), optionally via HTTPS, with brute-force
  lockout. Offers live status, JSON structure preview, backup & restore and a template editor for
  the remote sync.
* **Remote sync client:** Sends the collected data periodically or on demand via HTTP(S) POST to an
  external server. Payload format freely configurable via template, TLS certificate verification
  always active (with an optional own CA for internal/self-signed targets).
* **Admin 7 ready:** Uses the modern `jsonConfig` with responsive elements; the settings page is
  translated into all languages supported by ioBroker.

---

## Configuration

The configuration is done in the ioBroker admin interface, spread over three tabs.

### Tab 1: Settings & routes

* **MQTT target path (prefix):** The base namespace under which the mirrored objects are created.
  Must end with a dot (default: `mqtt.0.`). If the field is empty, `mqtt.0.` is used.
* **Update interval (s):** Periodic comparison of the `out`/`both` directions — only writes values
  that actually changed since the last run (no forced writes, that is the job of the force sync).
  Minimum 5 s, even if a smaller value is entered.
* **Log transfers:** Writes every synchronised value as an info line into the ioBroker log.
* **Force sync interval (min, 0 = off):** Bypasses the cache and compares all `out`/`both` values as
  well as all `in` values (mode `Single`) directly with the current value on the target side. It
  only writes where both actually differ and the source is active — to heal states that drifted
  apart, e.g. after a restart. The writes are staggered by 75 ms each so the radio budget
  (Zigbee/433 MHz) is not hit in a burst.
* **Staleness limit (min, 0 = off):** Default `1440` (24 h). Sources whose last update is older or
  that report a quality `q ≠ 0` are not mirrored on start, in the update interval and in the force
  sync. Can be overridden per mapping (field "Staleness").
* **Mappings:** The heart of the adapter.
    * **Source ID (ioBroker):** The original ioBroker state
      (e.g. `shelly.0.SHSW-25#D8BFC01A#1.Relay0.Switch`).
    * **Path suffix:** The desired MQTT path (e.g. `office/light/ceiling`). `/` automatically
      becomes `.`; characters ioBroker forbids in IDs are replaced by `_`.
    * **Direction:** `IOB -> MQTT`, `MQTT -> IOB` or `Both`.
    * **MQTT topic mode:** Affects **only the MQTT side**. See
      [Topic mode](#topic-mode-single-vs-dual) below. `Single` (default) is the original behaviour.
    * **Command suffix:** Only relevant for `Dual`, default `/set`. A leading slash is optional,
      nested suffixes (`/cmd/write`) are possible.
    * **Type:**
        * `Auto`: Determines the actual ioBroker data type of the target object (`boolean`,
          `number`) and converts accordingly — works in both directions.
        * `Round`: Rounds numeric values to the configured number of decimals.
        * `Bool->Num`: `true`/`false` → `1`/`0`.
        * `Num->Bool`: Treats `"0"`/`"false"`/`"off"`/empty as `false` and `"1"`/`"true"`/`"on"`
          as `true`, everything else via normal JavaScript truthiness.
        * With `Both`, `Bool->Num`/`Num->Bool` is swapped automatically for the reverse direction
          (never a value inversion, only the representation adapts to the respective side).
    * **Decimals:** Only relevant for `Round`, default 2.
    * **Unit:** Optional, for exports/remote sync and newly created target objects.
    * **Ack filter:** See [Ack filter](#ack-filter-for-states-without-a-device) below.
    * **Staleness (min):** Empty = global staleness limit, `0` = no age check for this entry (useful
      for devices that only report on value changes, e.g. window contacts).
    * **Sync mode:** See [Sync mode per mapping](#sync-mode-per-mapping) below.

  The mappings are shown as an expandable list with the path suffix as title. Each entry has two
  rows: at the top *what goes where* (source, path, direction, topic mode), at the bottom *how*
  (type, unit, ack filter, staleness, sync mode). Command suffix and decimals are only shown when
  they have an effect (topic mode `Dual` or type `Round`).

### Tab 2: Web dashboard

* **Web server port:** Default 8095.
* **Bind interface:** `0.0.0.0` (all interfaces, reachable from the LAN) or `127.0.0.1` (only
  locally on the ioBroker host).
* **Dashboard user name / password:** Credentials for the dashboard (HTTP basic auth). If the
  password stays empty, the web server is reachable **without access protection** — the adapter
  then explicitly warns in the log on start.
* **TLS certificate / TLS key (PEM, optional):** If both are set, the dashboard runs via HTTPS
  instead of HTTP and credentials are transmitted encrypted. Without TLS, basic auth credentials
  travel in plain text (Base64 is not encryption) — on a pure LAN a smaller but real risk.
* **Dashboard link:** The ready-made link to the dashboard is shown in the admin instance overview
  (icon next to the instance) and in the state `info.dashboardUrl`.

### Tab 3: Remote sync

* **Webhook URL:** The complete target URL, including any API key parameters.
* **Interval (min):** How often data is sent automatically.
* **Skip inactive sources:** Values considered inactive according to the staleness limit/quality
  are not sent to the webhook (default: off).
* **Trusted certificate / CA (PEM, optional):** Only needed for a self-signed/internal certificate
  of the sync target. Leave empty for a public certificate — then the normal system trust store
  applies. **TLS certificate verification is always active**, there is no way to switch it off; for
  internal targets the own certificate/CA is trusted specifically instead of disabling the check.
* **Check connection (button):** Runs a synchronisation immediately and shows the result directly
  in the admin. During this manual test the fingerprint of the loaded CA is logged additionally (if
  configured) — useful for troubleshooting certificate problems.

---

## Concepts in detail

### The web dashboard (backup & restore)

Reachable via the link in the instance overview or `info.dashboardUrl` (login required if a
password is set).

**Functions:**
1. **Status:** Watchdog, number of mappings, uptime.
2. **Backup & restore:**
   * **Download Backup (.json):** Downloads a `.json` file with the tree structure and the complete
     mapping configuration.
   * **Restore backup:** Uploads a previously saved file, replaces the mapping configuration and
     restarts the adapter. The upload must come from the same origin as the dashboard itself (CSRF
     protection) — an upload from a foreign web page is rejected.
3. **JSON structure preview:** Live preview of the generated MQTT tree structure.
4. **Remote sync configuration (template):** Format of the JSON object per state that is sent to
   the remote server.

### Security

* Without a dashboard password the web server is openly reachable — the adapter points this out
  explicitly in the log on start.
* After 10 failed login attempts the requesting IP is locked for 5 minutes. This lockout also
  survives an adapter restart (it is persisted).
* State-changing requests (e.g. backup upload) are only accepted if origin/referer match the own
  host — prevents a foreign web page from overwriting the configuration through the browser of a
  logged-in user.
* Recommendation: set a password and, if the dashboard is reachable outside a trusted LAN, also
  configure a TLS certificate/key.

### Ack filter for states without a device

For the direction ioBroker → MQTT, a value change is forwarded by default only if it is marked
with `ack: true` — which normally signals that a real device/bridge confirmed the value (e.g. a
Shelly relay). For `0_userdata.0.*` and `alias.0.*` there is no device that sets such a
confirmation — values written there, e.g. via the admin UI or a script, typically arrive with
`ack: false` and are not transmitted at all with the default filter. The filter applies to all
paths (event, start, update interval, force sync) — a never confirmed command to an offline device
therefore does not appear on MQTT as a state afterwards either.

For exactly such mappings set the ack filter to **"Also unconfirmed"**: every change is then
forwarded immediately, regardless of the `ack` flag. For mappings with a real device behind them
the default **"Confirmed only"** remains the right choice. The setting only affects the direction
IOB→MQTT; the reverse direction MQTT→IOB always requires a real broker confirmation.

### Topic mode: `single` vs. `dual`

The mode is set **per mapping**, not globally — different devices can therefore use different
conventions.

**Important distinction:** The mode affects **only the MQTT side**. The ioBroker state from the
"Source ID" field is always addressed directly, without any suffix — regardless of the mode. A
Shelly relay or a self-created `0_userdata` variable therefore needs no `/set` and gets none; only
the broker sees the separated topics.

| Side | Mode `Single` | Mode `Dual` |
|---|---|---|
| ioBroker (source ID) | `shelly.0.…Relay0.Switch` | `shelly.0.…Relay0.Switch` (identical) |
| MQTT state | `livingroom/light` | `livingroom/light` |
| MQTT command | `livingroom/light` (the same) | `livingroom/light/set` |

**`Single` (default, original behaviour):** Command and state share one topic. With
`Direction = Both`, the adapter therefore writes to the same topic it reads from.

**`Dual`:** Follows the common MQTT convention and separates both tasks:

| | Topic | Who writes |
|---|---|---|
| Command (write access) | `livingroom/light/set` | external client / dashboard |
| State (status report) | `livingroom/light` | `mqtt-plus`, as soon as the device confirms |

The flow with `Direction = Both` + `Dual`:

1. A command arrives on `…/light/set` and is written to the ioBroker state — with `ack: false`,
   i.e. as an unconfirmed control command.
2. The device switches and reports its new state back (`ack: true`).
3. `mqtt-plus` mirrors this confirmed report to the base topic `…/light`.

Write and read direction are therefore physically separate topics — a feedback loop between them
is structurally impossible, not just mitigated by protection mechanisms.

The resulting mapping of topic and `ack` flag:

| Function | MQTT topic | ioBroker `ack` | Purpose |
|---|---|---|---|
| Read / state (get) | `<prefix>/<path>` | `ack: true` (confirmed) | Actual state of the device |
| Set / command (set) | `<prefix>/<path>/set` | `ack: false` (command) | Switching request to the device |

In practice: an incoming command is always written to the ioBroker state with `ack: false` — as an
unconfirmed switching request that the responsible device adapter executes. Conversely, something
is only reported on the state topic once the device has confirmed the new state with `ack: true`.
A not yet confirmed command therefore does not appear there.

On the command topic itself the adapter accepts **every** incoming message in mode `Dual`,
regardless of the `ack` flag: many MQTT adapters deliberately forward `/set` messages as an
unconfirmed control command (`ack: false`), and no echo can arise there because `mqtt-plus` never
writes to the command topic itself. In mode `Single` the check for `ack: true` stays mandatory —
there an `ack: false` event would be the adapter's own write.

A command is also never discarded as "redundant" by the value cache: the cache only knows the last
command, not the device state. If the device was switched elsewhere in the meantime (button, app),
repeating the previous command must still reach the device.

With `Dual` the adapter creates both objects in the target tree, the base topic first and the
command topic below it. The JSON structure preview and the backup show both topics
(`full_topic` and `command_topic`) including the mode.

**Important — commands are never repeated with `Dual`:** A command topic is a command channel, not
a state store. Therefore both the initial comparison on adapter start and the force sync skip the
direction MQTT → IOB when `Dual` is set. Otherwise a (possibly retained) command lying there would
be executed again after every restart or every few minutes — a light switched off at the wall
switch would turn itself back on. In mode `Single` this comparison stays active because the topic
there actually holds the state.

### Echo protection for `both` mappings

With `dir: "both"` a topic serves as source and target at the same time — without a protection
mechanism, the adapter's own write would trigger a feedback loop. `mqtt-plus` prevents this on two
levels:

1. **MQTT → IOB** is always bound to `ack: true`. The MQTT adapter marks incoming broker messages
   with `ack: true`, whereas the adapter's own writes always use `ack: false` — so an `ack: false`
   event can never cause an own echo *on this path* and is ignored without risking a real
   confirmation.
2. **IOB → MQTT** also uses the ack check by default (see ack filter above) and therefore needs no
   additional echo protection. Only with `ackFilter: "any"` (where own `ack: false` writes can get
   through) the adapter briefly remembers (max. 10 seconds) the last value it wrote per target ID
   and discards an exactly matching echo — a real, different change is never blocked by this.

Result: real device confirmations get through reliably, also on `both` mappings, while real
self-loops (especially with `ackFilter: "any"`) are still prevented.

In topic mode `Dual` the question does not arise anyway: there the adapter writes to a different
topic than it reads from, so a self-echo cannot occur in the first place.

### Timestamps & staleness

Every ioBroker state has `ts` (last update, also with an unchanged value) and `lc` (last change).
Previously every mirror got `ts = now` on copy — after every restart and on every force sync. A
device dead for days therefore looked permanently active on the target side.

Since 1.6.0:

1. **Timestamps are passed through:** When a state is mirrored, `ts`, `lc` and `q` of the source are
   taken over (`c = "mqtt-plus"` marks the origin). Exception: commands in topic mode `Dual`
   (`/set`) get the current time — a command really is new.
2. **Active sources only:** Start, update interval and force sync only mirror a source if its `ts`
   is younger than the staleness limit and `q = 0`. Real change events always pass — the event
   itself proves the device is alive. Changes between "inactive" and "active again" are logged
   once.
3. **No blind rewriting:** Start and force sync read the target value first; if it already matches,
   nothing is written (no new `ts`, no MQTT message, no radio traffic).

**Note:** Some adapters only write on value changes — there `ts` stays unchanged for a long time
even with an active device. For such mappings set the field "Staleness" to `0` or choose a
generous value.

### Sync mode per mapping

| Mode | Behaviour | Use case |
|---|---|---|
| **Standard (changes only)** | Only real value changes, original timestamps, inactive sources rest. | Normal case |
| **Pass every update** | Like standard, but every new report of the source with the *same* value is mirrored too — `ts` moves along. If the device goes silent, the target honestly becomes stale. | Sensors that measure correctly but whose value hardly changes and would otherwise look "greyed out" on the target side |
| **Force (as before 1.6.0)** | No staleness check, no target comparison; start and force sync always write with `ts = now`. | When the target must look fresh at all costs |

**Caution with "Force (as before 1.6.0)":** The target looks permanently active even if the device
stopped reporting long ago — exactly the behaviour 1.6.0 removed. Where the device reports
regularly (also with the same value), "Pass every update" is the more honest choice. "Pass every
update" produces more MQTT messages in return (one per device report).

### Remote sync & templates

The adapter sends an array of objects to the configured webhook. With very many mappings the
transfer is automatically split into chunks of 200 entries each, so a single timeout does not
discard the complete payload; if a chunk fails, the error message shows how many values already
arrived. Network errors and 5xx responses are retried up to twice with increasing delay, TLS
certificate errors and 4xx responses are not (a retry would not change them anyway).

**Available placeholders:**
* `%ID%`: The ioBroker source ID (e.g. `shelly.0.relay`).
* `%MQTT%`: The defined MQTT suffix (e.g. `light/kitchen`).
* `%PREFIX%`: The base path defined in the settings (e.g. `mqtt.0.`).
* `%VAL%`: The current value (number, string or boolean).
* `%TS%`: Timestamp of the last update (`ts`, ms since 1970).
* `%LC%`: Timestamp of the last value change (`lc`).
* `%ACK%`: `true` if the value is confirmed by the device.
* `%Q%`: Quality of the value (`0` = good).
* `%UNIT%`: The defined unit.
* `%DIR%`: The configured direction (`in`, `out`, `both`).

All placeholders except `%VAL%`/`%TS%`/`%LC%`/`%ACK%`/`%Q%` are JSON-escaped automatically when
inserted — a quote character in an ID no longer breaks the generated JSON.

**Default template:**
```json
{
  "id": "%ID%",
  "topic": "%MQTT%",
  "value": %VAL%,
  "ts": %TS%,
  "unit": "%UNIT%",
  "prefix": "%PREFIX%",
  "dir": "%DIR%"
}
```

---

## Diagnostic states

Besides the configuration tabs, the adapter creates the following status states:

| State | Meaning |
|---|---|
| `info.connection` | Web server running/not running |
| `info.version` | Currently running adapter version |
| `info.status` | Machine-readable status (`Running`, `Cycle OK`, `Force-Sync OK`, …) |
| `info.lastCycle` | Unix timestamp of the last completed sync cycle |
| `info.lastSyncStatus` | Result of the last remote sync run (success/error text) |
| `info.dashboardUrl` | Ready-made link to the web dashboard |
| `info.authLockouts` | Internal: active dashboard login lockouts (survives restarts) |
| `config.syncTemplate` | Template for the remote sync (editable in the dashboard) |
| `watchdog` | Free-text status line (historic, for overview in the object tree) |
