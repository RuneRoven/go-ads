# ADS Protocol and Library Documentation

## Table of Contents

- [ADS Protocol Overview](#ads-protocol-overview)
- [Protocol Architecture](#protocol-architecture)
- [AMS/TCP Packet Structure](#amstcp-packet-structure)
- [ADS Commands](#ads-commands)
- [Addressing and Routing](#addressing-and-routing)
- [Symbol System](#symbol-system)
- [Library Architecture](#library-architecture)
- [API Reference](#api-reference)
- [Connection Lifecycle](#connection-lifecycle)
- [Symbol Discovery Limitations](#symbol-discovery-limitations)
- [Secure ADS (ADS over TLS)](#secure-ads-ads-over-tls)
- [Data Types](#data-types)

---

## ADS Protocol Overview

ADS (Automation Device Specification) is a transport-independent protocol developed by Beckhoff for communicating with TwinCAT automation systems. It is the native communication protocol used by all TwinCAT PLCs and provides access to PLC variables, device information, and real-time notifications.

ADS sits on top of AMS (Automation Message Specification), which handles addressing and routing between devices. In practice, AMS/ADS packets are transported over TCP on port **48898**.

### Key Characteristics

- **Binary protocol** — all data is encoded in little-endian byte order
- **Request/response model** — each request gets a matching response identified by an invoke ID
- **Asynchronous notifications** — the PLC can push data changes to clients without polling
- **Symbol-based access** — variables can be read/written by name, not just raw memory addresses
- **Transport-independent** — while TCP is most common, ADS can run over other transports

---

## Protocol Architecture

```text
┌─────────────────────────────────────┐
│          Application Layer          │
│   (Symbol reads, writes, notifs)    │
├─────────────────────────────────────┤
│           ADS Commands              │
│  (Read, Write, Notification, etc.)  │
├─────────────────────────────────────┤
│          AMS Header (32 bytes)      │
│   (Source/Target addressing,        │
│    Command ID, Invoke ID)           │
├─────────────────────────────────────┤
│        AMS/TCP Header (6 bytes)     │
│   (Packet length, system flag)      │
├─────────────────────────────────────┤
│              TCP                    │
│         (Port 48898)                │
└─────────────────────────────────────┘
```

---

## AMS/TCP Packet Structure

Every AMS/TCP packet consists of three parts: an AMS/TCP header, an AMS header, and command-specific data.

### AMS/TCP Header (6 bytes)

| Offset | Size | Field    | Description                                    |
|--------|------|----------|------------------------------------------------|
| 0      | 1    | Reserved | Always 0 for normal ADS packets                |
| 1      | 1    | System   | 0 = normal ADS, non-zero = system/router message |
| 2      | 4    | Length   | Length of the AMS header + ADS data (excludes this 6-byte header) |

System messages (System > 0) are used for router-level operations such as requesting the local AMS NetID when connecting to a local TwinCAT runtime.

### AMS Header (32 bytes)

| Offset | Size | Field      | Description                              |
|--------|------|------------|------------------------------------------|
| 0      | 8    | Target     | Target AMS address (6-byte NetID + 2-byte port) |
| 8      | 8    | Source     | Source AMS address (6-byte NetID + 2-byte port) |
| 16     | 2    | Command    | ADS command ID (see table below)         |
| 18     | 2    | State      | State flags (4 = ADS request with response) |
| 20     | 4    | Length     | Length of the ADS data following this header |
| 24     | 4    | Error Code | AMS error code (0 = no error)            |
| 28     | 4    | Invoke ID  | Unique ID to match responses to requests |

The **Invoke ID** is a counter that increments with each request. The PLC echoes it back in the response, allowing the client to match responses to their original requests even when multiple requests are in flight.

### ADS Data

The ADS data section varies by command. Each command has its own request and response format, documented in the [ADS Commands](#ads-commands) section.

---

## ADS Commands

| ID | Command                      | Description                                       |
|----|------------------------------|---------------------------------------------------|
| 0  | Invalid                      | Invalid / not used                                |
| 1  | ReadDeviceInfo               | Read device name and version                      |
| 2  | Read                         | Read data by index group and offset               |
| 3  | Write                        | Write data by index group and offset              |
| 4  | ReadState                    | Read ADS and device state                         |
| 5  | WriteControl                 | Change ADS/device state                           |
| 6  | AddDeviceNotification        | Subscribe to data change notifications            |
| 7  | DeleteDeviceNotification     | Unsubscribe from notifications                    |
| 8  | DeviceNotification           | Notification data pushed from PLC to client       |
| 9  | ReadWrite                    | Combined read and write in a single round-trip    |

### Read (Command 2)

Reads data from the PLC at a given index group and offset.

**Request:**

| Size | Field  | Description              |
|------|--------|--------------------------|
| 4    | Group  | Index group              |
| 4    | Offset | Index offset             |
| 4    | Length | Number of bytes to read  |

**Response:**

| Size | Field  | Description              |
|------|--------|--------------------------|
| 4    | Error  | ADS return code          |
| 4    | Length | Actual bytes returned    |
| n    | Data   | The requested data       |

### Write (Command 3)

Writes data to the PLC at a given index group and offset.

**Request:**

| Size | Field  | Description              |
|------|--------|--------------------------|
| 4    | Group  | Index group              |
| 4    | Offset | Index offset             |
| 4    | Length | Number of bytes to write |
| n    | Data   | The data to write        |

**Response:**

| Size | Field  | Description     |
|------|--------|-----------------|
| 4    | Result | ADS return code |

The Write response contains **only a single 4-byte return code** — there is no separate success field and no data payload. A Result value of `0x0000` (`ReturnCodeNoErrors`) indicates success. Any non-zero value indicates an error. This is confirmed by the Beckhoff ADS specification and the reference C++ implementation (`AoEResponseHeader` contains only `leResult`).

Note that errors can occur at two levels:
- **AMS level** — the Error Code field in the 32-byte AMS header (e.g., routing failures, target not found)
- **ADS level** — the Result field in the command response (e.g., invalid group, access denied, symbol not found)

### ReadWrite (Command 9)

Sends write data and receives read data in a single round-trip. Used extensively for symbol handle operations and sum commands.

**Request:**

| Size | Field       | Description               |
|------|-------------|---------------------------|
| 4    | Group       | Index group               |
| 4    | Offset      | Index offset              |
| 4    | ReadLength  | Expected read bytes       |
| 4    | WriteLength | Number of bytes to write  |
| n    | Data        | The data to write         |

**Response:**

| Size | Field  | Description           |
|------|--------|-----------------------|
| 4    | Error  | ADS return code       |
| 4    | Length | Actual bytes returned |
| n    | Data   | The read data         |

### AddDeviceNotification (Command 6)

Subscribes to change notifications for a data area. The PLC will send DeviceNotification messages when the data changes.

**Request:**

| Size | Field            | Description                            |
|------|------------------|----------------------------------------|
| 4    | Group            | Index group                            |
| 4    | Offset           | Index offset                           |
| 4    | Length            | Data length to monitor                 |
| 4    | TransmissionMode | When to send notifications (see below) |
| 4    | MaxDelay         | Maximum delay before sending (100ns units) |
| 4    | CycleTime        | Check interval (100ns units)           |
| 16   | Reserved         | Must be zero                           |

**Transmission Modes:**

| Value | Mode               | Description                                        |
|-------|--------------------|----------------------------------------------------|
| 0     | NoTransmission     | No notifications                                   |
| 1     | ClientCycle        | Client-side cyclic check (deprecated)              |
| 2     | ClientOnChange     | Client-side on-change (deprecated)                 |
| 3     | ServerCycle        | Server sends at fixed intervals                    |
| 4     | ServerOnChange     | Server sends only when value changes               |
| 5     | ServerCycle2       | ServerCycle with timestamp (TwinCAT 3 only)        |
| 6     | ServerOnChange2    | ServerOnChange with timestamp (TwinCAT 3 only)     |
| 10    | Client1Request     | Single notification then auto-delete               |

**Response:**

| Size | Field  | Description              |
|------|--------|--------------------------|
| 4    | Error  | ADS return code          |
| 4    | Handle | Notification handle      |

### DeviceNotification (Command 8)

Pushed from the PLC to the client. Contains one or more timestamped data samples.

**Structure:**

```text
NotificationStream:
  Length (4 bytes) — total data length
  Stamps (4 bytes) — number of stamp headers

For each stamp:
  StampHeader:
    Timestamp (8 bytes) — Windows FILETIME (100ns since 1601-01-01)
    Samples   (4 bytes) — number of samples in this stamp

  For each sample:
    NotificationSample:
      Handle (4 bytes) — notification handle (matches AddDeviceNotification response)
      Size   (4 bytes) — data size
      Data   (n bytes) — the notification payload
```

The timestamp uses Windows FILETIME format: 100-nanosecond intervals since January 1, 1601. To convert to Unix time: `unixSeconds = (filetime / 10000000) - 11644473600`.

---

## Addressing and Routing

### AMS NetID

Every AMS device is identified by a 6-byte **AMS NetID**, typically displayed in dotted notation (e.g., `5.1.2.3.1.1`). By convention:
- The first 4 bytes often match the device's IP address
- The last 2 bytes are typically `1.1`

For example, a PLC at IP `192.168.1.100` might have AMS NetID `192.168.1.100.1.1`.

### AMS Port

Each service on a TwinCAT device listens on a specific AMS port:

| Port | Service                          |
|------|----------------------------------|
| 100  | Logger                           |
| 200  | Real-time system                 |
| 300  | I/O                              |
| 400  | SPS (TwinCAT 2 legacy)          |
| 500  | NC                               |
| 801  | PLC Runtime 1 (TwinCAT 2 & 3)  |
| 811  | PLC Runtime 2                    |
| 821  | PLC Runtime 3                    |
| 831  | PLC Runtime 4                    |
| 851  | PLC Runtime 1 (TwinCAT 3 preferred) |

### Index Groups

ADS uses **index group** and **index offset** pairs to address data. Several reserved index groups provide access to the symbol system:

| Group    | Name                           | Purpose                                       |
|----------|--------------------------------|-----------------------------------------------|
| 0xF000   | SymbolTab                      | Symbol table                                  |
| 0xF003   | SymbolHandleByName             | Get a handle for a symbol name                |
| 0xF004   | SymbolValueByName              | Read symbol value by name (one-shot)          |
| 0xF005   | SymbolValueByHandle            | Read/write symbol value using handle          |
| 0xF006   | SymbolReleaseHandle            | Release a previously acquired handle          |
| 0xF007   | SymbolInfoByName               | Get symbol metadata by name                   |
| 0xF008   | SymbolVersion                  | Read symbol version (changes on PLC download) |
| 0xF00B   | SymbolUpload                   | Download full symbol table from PLC           |
| 0xF00C   | SymbolUploadInfo               | Get symbol table size info                    |
| 0xF00E   | SymbolDataTypeUpload           | Download all datatype definitions             |
| 0xF00F   | SymbolUploadInfo2              | Extended symbol upload info (TwinCAT 3)       |
| 0xF080   | SumupRead                      | Batch read multiple values                    |
| 0xF081   | SumupWrite                     | Batch write multiple values                   |
| 0xF082   | SumupReadWrite                 | Batch read+write                              |
| 0xF084   | SumupReadEx2                   | Batch read with per-item error codes          |
| 0xF085   | SumupAddDeviceNotification     | Batch add notifications                       |
| 0xF086   | SumupDeleteDeviceNotification  | Batch delete notifications                    |

### Route Registration

Before a remote client can communicate with a PLC, a **route** must exist on both sides. The PLC needs to know the client's AMS NetID and how to reach it (IP address).

Routes can be added via the Beckhoff UDP discovery/route protocol on port **48899**. The packet format uses a tag-based structure:

1. Header: cookie (magic number `0x71146603`) + invoke ID + service ID + AMS address + tag count
2. Tags: NetID, password, computer name, route name, username

The PLC responds with a success/error code. Once the route is registered, TCP-based ADS communication on port 48898 can proceed.

---

## Symbol System

TwinCAT PLCs expose their variables through a **symbol system**. Instead of requiring raw memory addresses, clients can reference variables by name (e.g., `MAIN.myVar`).

### Symbol Discovery

On connect, the library performs a full symbol discovery:

1. **Read symbol version** — `GroupSymbolVersion` (0xF008) returns a version number that changes whenever the PLC program is downloaded. This allows detecting when symbols are stale.

2. **Read symbol upload info** — `GroupSymbolUploadInfo2` (0xF00F) or `GroupSymbolUploadInfo` (0xF00C) returns the total byte counts for symbols and datatypes.

3. **Download datatypes** — `GroupSymbolDataTypeUpload` (0xF00E) returns all datatype definitions. Each datatype entry contains:
   - Name, type, comment
   - Size, offset, flags
   - Array dimensions and sub-items (for structs)

4. **Download symbols** — `GroupSymbolUpload` (0xF00B) returns all top-level symbols. Each symbol entry contains:
   - Name (e.g., `MAIN.myVar`)
   - Datatype name (e.g., `INT`, `REAL`, `MyStruct`)
   - Index group, index offset, size

5. **Build symbol tree** — datatypes are used to expand structured symbols into a tree. For example, `MAIN.motor` of type `ST_Motor` with fields `speed` and `position` creates child symbols `MAIN.motor.speed` and `MAIN.motor.position`.

> **Important:** Symbol discovery downloads the entire symbol and datatype tables in single ADS requests. See [Symbol Discovery Limitations](#symbol-discovery-limitations) for performance implications.

### Symbol Handles

To read or write a symbol by name, you first acquire a **handle** (a uint32 identifier):

1. **Get handle**: `WriteRead` with `GroupSymbolHandleByName` (0xF003), writing the symbol name
2. **Use handle**: `Read`/`Write` with `GroupSymbolValueByHandle` (0xF005) using the handle as offset
3. **Release handle**: `Write` with `GroupSymbolReleaseHandle` (0xF006) when done

Handles are cached per symbol — the library acquires them lazily on first access and releases them all on disconnect.

### Data Types

The library can parse and serialize these PLC data types:

| PLC Type       | Size    | Go Representation          |
|----------------|---------|----------------------------|
| BOOL           | 1 byte  | "true" / "false"           |
| BYTE, USINT    | 1 byte  | Unsigned 0-255             |
| SINT           | 1 byte  | Signed -128 to 127         |
| UINT, WORD     | 2 bytes | Unsigned 0-65535           |
| INT            | 2 bytes | Signed -32768 to 32767     |
| UDINT, DWORD   | 4 bytes | Unsigned 32-bit            |
| DINT           | 4 bytes | Signed 32-bit              |
| REAL           | 4 bytes | 32-bit float               |
| LREAL          | 8 bytes | 64-bit float               |
| STRING         | varies  | Null-terminated string     |
| TIME           | 4 bytes | Milliseconds, formatted as `HH:MM:SS.sss` |
| TOD            | 4 bytes | Time of day, formatted as `HH:MM` |
| DATE           | 4 bytes | Seconds since epoch, formatted as `YYYY-MM-DD` |
| DT             | 4 bytes | Date+time, formatted as `YYYY-MM-DD HH:MM:SS` |

Structured types (STRUCTs, FUNCTION_BLOCKs) and arrays are recursively expanded into child symbols and serialized as JSON.

---

## Library Architecture

```text
┌──────────────────────────────────────────────────┐
│                   Application                     │
│  ReadFromSymbol / WriteToSymbol / Notifications   │
├──────────────────────────────────────────────────┤
│                  ads.go / symbols.go              │
│         Symbol resolution, caching, parsing       │
├──────────────────────────────────────────────────┤
│               command*.go files                   │
│    Read, Write, ReadWrite, Notifications,         │
│    SumRead, SumNotification, DeviceInfo, State    │
├──────────────────────────────────────────────────┤
│                    comm.go                        │
│     sendRequest, listen, handleReceive,           │
│     transmitWorker (goroutines)                   │
├──────────────────────────────────────────────────┤
│                    ams.go                         │
│          AMS/TCP packet encoding                  │
├──────────────────────────────────────────────────┤
│                 connection.go                     │
│    TCP dial, connect, reconnect, close            │
├──────────────────────────────────────────────────┤
│                  route.go                         │
│        UDP route registration (port 48899)        │
└──────────────────────────────────────────────────┘
```

### Concurrency Model

The library uses three goroutines per connection:

1. **listen** — reads packets from TCP, parses AMS headers, and dispatches:
   - Notification packets (Command 8) are handled directly
   - Response packets are routed to the waiting request goroutine via a channel keyed by invoke ID

2. **transmitWorker** — serializes outgoing packets onto the TCP connection from a send channel

3. **handleReceive** — spawned per incoming response, delivers data to the correct request channel

Requests use a `map[uint32]chan []byte` keyed by invoke ID. Each `sendRequest` call:
1. Allocates a new invoke ID (atomic counter)
2. Creates a response channel in the map
3. Sends the encoded packet via the send channel
4. Blocks waiting on the response channel (with timeout)
5. Cleans up the map entry on return

### Reconnection

When TCP read errors are detected (including TCP keepalive failures), the library automatically:

1. Closes the old TCP connection
2. Stops listen and transmitWorker goroutines
3. Retries TCP dial in a loop (configurable interval, default 5s)
4. Re-loads symbols based on discovery mode:
   - If `LoadSymbols()` was used: re-downloads the full symbol table
   - If on-demand only: re-resolves only the specific symbols that were previously accessed
   - If no symbols were loaded: reads symbol version only
5. Re-subscribes to all previously registered notifications

TCP keepalive is configured aggressively (idle=3s, interval=2s, count=5) so cable disconnections are detected within ~13 seconds.

---

## API Reference

### Connection Management

#### `NewConnection(ctx, ip, port, netid, amsPort, localNetID, localPort, requestTimeout) (*Connection, error)`

Creates a new ADS connection configuration. Does not connect yet.

- `ip` — PLC IP address
- `port` — TCP port (typically `48898`)
- `netid` — Target PLC AMS NetID in dotted notation (e.g., `"5.1.2.3.1.1"`)
- `amsPort` — Target AMS port (e.g., `851` for TwinCAT 3 Runtime 1)
- `localNetID` — Source AMS NetID; use `"auto"` to derive from local IP
- `localPort` — Source AMS port (e.g., `10500`)
- `requestTimeout` — Timeout for individual ADS requests (0 defaults to 5000ms)

#### `Connect(local bool) error`

Establishes the TCP connection and starts background goroutines. Does **not** load the symbol table — symbols are resolved on-demand when first accessed, or you can call `LoadSymbols()` / `LoadSymbolsSlow()` explicitly.

- `local` — set `true` when connecting to a TwinCAT runtime on the same machine (uses loopback and queries local NetID via system message)

#### `Close()`

Gracefully shuts down: deletes all notification handles, releases symbol handles, closes TCP, and waits for goroutines to finish.

#### `Reconnect() error`

Re-establishes the connection after a failure. Called automatically on TCP errors, but can also be called manually. If full discovery was performed, it re-loads the full symbol table. If only on-demand symbols were resolved, it re-resolves only those specific symbols.

#### `IsDisconnected() bool`

Returns whether the connection is currently in a disconnected state.

### Symbol Discovery

#### `LoadSymbols() error`

Performs full symbol and datatype discovery from the PLC. Downloads the entire symbol table and datatype definitions in single requests. After calling this:
- `ListSymbols()` returns all symbols
- Struct/array children are available
- Write operations with type aliases work

**Warning:** This locks the PLC task during the download. For large programs, consider `LoadSymbolsSlow()`.

#### `LoadSymbolsSlow(cfg SlowDiscoveryConfig) error`

Downloads the full symbol table in chunks with configurable delays between chunks, to minimize disruption to the PLC's real-time task. Falls back to single-request downloads if the PLC does not support offset-based chunked reads.

```go
conn.LoadSymbolsSlow(ads.SlowDiscoveryConfig{
    ChunkSize:  4096,            // bytes per chunk (default: 4096)
    ChunkDelay: 100*time.Millisecond, // delay between chunks (default: 100ms)
})
```

### Browse Mode (Partial Discovery)

Browse mode allows lazy navigation of the PLC symbol hierarchy without downloading everything at once. It splits discovery into two independent downloads:

| Mode | API | Downloads | Can List | Can Browse Children | Can Read Values |
|------|-----|-----------|----------|--------------------|----|
| None (default) | `Connect()` | Nothing | No | No | Yes (on-demand) |
| Symbol list only | `LoadSymbolList()` | Symbol table (~small) | Top-level | Heuristic only | Yes (on-demand) |
| Symbol list + types | `LoadSymbolList()` + `LoadDataTypes()` | Both tables | All | Yes (fully expanded) | Yes |
| Full discovery | `LoadSymbols()` / `LoadSymbolsSlow()` | Both tables | All | Yes | Yes |

#### `LoadSymbolList(cfg SlowDiscoveryConfig) error`

Downloads only the symbol table (0xF00B) in chunks. This is the smaller of the two tables and enables browsing top-level symbol names via `BrowseSymbols()`. If `LoadDataTypes()` was already called, struct children are retroactively expanded.

```go
conn.LoadSymbolList(ads.SlowDiscoveryConfig{})
entries, _ := conn.BrowseSymbols("") // list root entries
```

#### `LoadDataTypes(cfg SlowDiscoveryConfig) error`

Downloads only the datatype table (0xF00E) in chunks. When combined with `LoadSymbolList()`, enables full struct/array child expansion. If the symbol list was already loaded, children are retroactively expanded.

```go
conn.LoadDataTypes(ads.SlowDiscoveryConfig{})
entries, _ := conn.BrowseSymbols("MAIN.motor") // list struct children
```

#### `BrowseSymbols(path string) ([]SymbolBrowseEntry, error)`

Returns browsable entries at the given path. If path is empty, returns root-level groupings. Requires `LoadSymbolList()` or `LoadSymbols()` to have been called first.

Each `SymbolBrowseEntry` contains:
- `Name` — short name (e.g., `"motor"`)
- `FullName` — full path (e.g., `"MAIN.motor"`)
- `DataType` — type name (e.g., `"ST_Motor"`, `"INT"`)
- `Size` — byte size
- `HasChildren` — true if the symbol has children (struct/array)
- `Comment` — PLC comment

### Reading and Writing

#### `ReadFromSymbol(symbolName string) (string, error)`

Reads a PLC variable by name and returns its parsed string value. If the symbol has not been discovered yet, it is resolved on-demand from the PLC (adds 2 ADS round-trips on first access, cached afterward). Includes a minimum update interval cache (50ms default) to avoid excessive reads.

#### `WriteToSymbol(symbolName string, value string) error`

Writes a value to a PLC variable by name. The string value is converted to the appropriate binary format based on the symbol's datatype. Symbols are resolved on-demand if not already loaded. Standard PLC types (BOOL, INT, REAL, STRING, etc.) work in on-demand mode. User-defined type aliases require `LoadSymbols()` first.

#### `ReadMultipleSymbols(names []string) (map[string]string, error)`

Reads multiple symbols in a single ADS round-trip using the SumRead command. Returns a map of symbol name to parsed value. Falls back to individual reads on older PLCs that don't support sum commands.

### Symbol Information

#### `ListSymbols() (map[string]*Symbol, error)`

Returns the full symbol table. Requires `LoadSymbols()` or `LoadSymbolsSlow()` to have been called first — returns an error otherwise. Keys are fully qualified symbol names (e.g., `"MAIN.myVar"`, `"MAIN.motor.speed"`).

#### `GetSymbol(symbolName string) (*Symbol, error)`

Returns the Symbol struct for a given name, acquiring a handle from the PLC if needed.

#### `RefreshSymbols() error`

Checks if the PLC's symbol version has changed (e.g., after a new program download) and reloads the symbol table if so.

#### `CheckSymbolVersion() (changed bool, err error)`

Checks if the symbol version has changed without reloading.

### Notifications

#### `AddSymbolNotification(symbolName, maxDelay, cycleTime int, transMode TransMode, ch chan *Update) error`

Subscribes to value changes on a single symbol. Updates are delivered to the provided channel.

#### `AddSymbolNotifications(configs []NotificationConfig, ch chan *Update) error`

Subscribes to multiple symbol notifications in a single ADS round-trip. All updates are delivered to the same channel. Notification configs are stored internally for automatic re-subscribe after reconnection.

### Device Information

#### `ReadDeviceInfo() (DeviceInfo, error)`

Returns the device name and TwinCAT version (major, minor, build).

#### `ReadState() (states, error)`

Returns the current ADS state (Run, Stop, Config, etc.) and device state.

### Route Management

#### `AddRemoteRoute(remoteHost, localNetId, routeName, computerName, username, password string) error`

Registers a route on the remote PLC via UDP (port 48899). This is needed before the PLC can send responses back to this client.

### Low-Level Access

#### `Read(group, offset, length uint32) ([]byte, error)`

Raw ADS Read by index group and offset.

#### `Write(group, offset uint32, data []byte) error`

Raw ADS Write by index group and offset.

#### `WriteRead(group, offset, readLength uint32, data []byte) ([]byte, error)`

Raw ADS ReadWrite — sends data and reads a response in one round-trip.

#### `SumRead(requests []SumReadRequest) ([]SumReadResult, error)`

Batch read using `GroupSumupReadEx2`. Returns per-item results with error codes and data. Falls back to individual reads on older PLCs.

#### `SumAddDeviceNotification(requests []SumNotificationRequest) (handles, errors, err)`

Batch add notifications using `GroupSumupAddDeviceNotification`. Falls back to individual calls and automatically downgrades TwinCAT 3 transmission modes (v2) to v1 equivalents for older PLCs.

#### `SumDeleteDeviceNotification(handles []uint32) ([]ReturnCode, error)`

Batch delete notifications using `GroupSumupDeleteDeviceNotification`.

---

## Connection Lifecycle

```text
1. NewConnection(...)          — configure target, source, timeouts
       │
2. [AddRemoteRoute(...)]       — optional: register route on PLC via UDP
       │
3. Connect(false)              — TCP dial → start goroutines (no symbol loading)
       │
4. [LoadSymbols()]             — optional: full discovery for ListSymbols/struct access
   [LoadSymbolsSlow(cfg)]      — optional: chunked discovery (PLC-friendly)
   [LoadSymbolList(cfg)]       — optional: browse mode (symbol names only)
   [LoadDataTypes(cfg)]        — optional: add struct expansion to browse mode
       │
5. ReadFromSymbol(...)         — symbols resolved on-demand if not discovered
   BrowseSymbols(path)         — navigate symbol hierarchy (requires step 4)
   WriteToSymbol(...)
   AddSymbolNotifications(...)
       │
   ┌───┴──── (TCP error detected) ────┐
   │                                   │
   │  Reconnect()                      │
   │  - re-dial TCP                    │
   │  - re-resolve symbols (mode-aware)│
   │  - re-subscribe notifications     │
   │                                   │
   └───────────────────────────────────┘
       │
6. Close()                     — delete notifications → release handles → close TCP
```

---

## Symbol Discovery Limitations

Full symbol discovery (triggered by calling `LoadSymbols()`) downloads the entire symbol table and all datatype definitions from the PLC in two large ADS Read requests. While convenient, this has important performance and sizing implications.

### PLC Real-Time Impact

The PLC task is **locked for every ADS request**. While the PLC is assembling a response, it pauses its real-time cycle. For a small PLC program this is negligible, but for large programs with thousands of symbols, the symbol and datatype upload responses can be substantial and cause measurable **cycle jitter**.

This means:
- A full symbol discovery on a large PLC program will cause a brief real-time pause
- The pause duration scales with symbol table size
- Motion control or other time-critical applications may be affected
- This typically only happens on connect/reconnect, not during normal operation

### AMS Router Buffer Limit

The ADS response must pass through the AMS router, which has a **default buffer size of 2 MB** (2,097,152 bytes, configurable via `ADS.Config::RESPONSE_SIZE_LIMIT`). If the combined symbol table or datatype definitions exceed this limit, the upload will fail.

PLC programs with very large numbers of symbols, deeply nested structs, or extensive arrays can exceed this limit. Symptoms include truncated responses or ADS errors during `Connect()`.

### Sizing Guidelines

| PLC Program Size | Typical Symbol Table | Impact            |
|------------------|---------------------|-------------------|
| Small (<100 symbols) | < 50 KB          | Negligible        |
| Medium (100-1000)    | 50 KB - 500 KB   | Minor jitter      |
| Large (1000-5000)    | 500 KB - 2 MB    | Noticeable jitter |
| Very large (>5000)   | > 2 MB           | May exceed buffer |

### Recommendations

- **Small/medium PLC programs**: Full discovery (the default) is fine and the most convenient approach.
- **Large PLC programs**: If real-time jitter during connect is a concern, consider increasing the PLC task cycle time temporarily during the connection phase, or schedule connects during non-critical periods.
- **Very large PLC programs**: If the symbol table exceeds the 2 MB AMS router buffer, you have two options:
  1. Increase the router buffer size on the PLC (`ADS.Config::RESPONSE_SIZE_LIMIT`)
  2. Use selective symbol access instead — query individual symbols by name using `GroupSymbolInfoByNameEx` (0xF009) rather than downloading the entire table
- **Sum commands**: Beckhoff recommends not exceeding **500 sub-commands** in a single sum request (SumRead, SumAddDeviceNotification, etc.) to avoid excessive real-time jitter.

### Is Full Discovery Required?

**No.** Full symbol discovery is a convenience, not a protocol requirement. The ADS protocol provides multiple ways to work with symbols without downloading the entire table:

| Approach | Index Group | Description |
|----------|------------|-------------|
| Full upload | 0xF00B + 0xF00E | Download entire symbol + datatype tables. Convenient but heavy. |
| Handle by name | 0xF003 | Get a handle for a single symbol by name. Lightweight. |
| Value by name | 0xF004 | Read a symbol value directly by name in one shot. No handle needed. |
| Info by name | 0xF009 | Get metadata (size, type, group, offset) for a single symbol. |
| Direct access | Any group/offset | If you already know the index group and offset, no discovery needed at all. |

For applications that only need a small subset of symbols, querying individual symbols by name (`GroupSymbolHandleByName` + `GroupSymbolValueByHandle`) avoids the cost of full discovery entirely. This is the recommended approach for large PLC programs where full discovery would cause unacceptable real-time jitter or exceed the AMS router buffer.

The Beckhoff reference C++ ADS library supports both modes — it can download the full symbol table or resolve individual symbols on demand.

---

## Secure ADS (ADS over TLS)

Secure ADS adds TLS encryption to the standard ADS protocol. It was introduced in **TwinCAT 3.1 Build 4024.0** and is included with TC1000 at no additional cost.

> **Note:** This library does **not** currently implement Secure ADS. It uses plain TCP on port 48898. This section documents the protocol for reference.

### Overview

Secure ADS creates an encrypted tunnel for all AMS/ADS communication using **TLS 1.2 with mutual authentication (mTLS)** — both client and server present certificates. Programs using standard ADS do not need modification; Secure ADS is transparent at the TwinCAT router level.

### Port

Secure ADS uses **TCP port 8016** instead of the standard port 48898.

| Protocol     | TCP Port | UDP Port |
|-------------|----------|----------|
| Standard ADS | 48898    | 48899 (route management) |
| Secure ADS   | 8016     | —        |

### Connection Sequence

```text
1. TCP connect to port 8016
2. TLS 1.2 handshake (mutual TLS — both sides authenticate)
3. TlsConnectInfo request (64-512 bytes, inside TLS tunnel)
4. TlsConnectInfo response (inside TLS tunnel)
5. AMS/ADS frames (inside TLS tunnel)
```

### Key Protocol Difference

Unlike standard ADS, Secure ADS **omits the 6-byte AMS/TCP framing header** inside the TLS tunnel. The TLS record layer provides framing instead, and the receiver determines frame boundaries from the AMS header's Length field.

### Authentication Modes

Secure ADS supports three certificate/authentication methods:

#### 1. Self-Signed Certificates (SSC)

- TwinCAT auto-generates a self-signed certificate on first startup
- Trust is established via **TOFU (Trust On First Use)** with SHA-256 fingerprint pinning
- Certificate validity: 1/1/2000 to 1/1/2061 (intentionally wide to avoid clock issues)
- Simplest to set up; good for initial deployments and small systems

#### 2. Pre-Shared Keys (PSK)

- Uses TLS-PSK with identity and password-derived keys
- No certificates required
- Keys are stored in configuration files (not hashed — keep them secure)
- PSKs have no expiration dates
- Suitable for maintenance staff access and simple setups

#### 3. Customer-Provided Certificates (Shared CA)

- Both peers trust certificates issued by a shared Certificate Authority
- Enables dynamic constellations — any device trusting the same CA can communicate without prior per-device configuration
- Most flexible for large deployments
- Requires proactive certificate renewal before expiration

### Security Considerations

- All methods require keeping secrets (private keys, PSKs) isolated and protected
- If secrets are compromised, the entire system must be re-configured to restore integrity
- Two ADS-specific TLS error codes exist:
  - `0x1D` (`ReturnCodeGlobalTlsSendError`): "TLS send error — secure ADS connection failed"
  - `0x1E` (`ReturnCodeGlobalAccessDenied`): "Access denied — secure ADS access denied"

### References

- [Beckhoff InfoSys: Secure ADS](https://infosys.beckhoff.com/content/1033/tc3_grundlagen/6798095243.html)
- [Beckhoff Secure ADS Manual (PDF)](https://download.beckhoff.com/download/document/automation/twincat3/Secure_ADS_EN.pdf)

---

### Error Handling

All ADS responses include a `ReturnCode`. The library defines comprehensive return codes covering:

- **Global errors** (0x01-0x1E) — internal errors, routing failures, timeouts
- **Router errors** (0x500-0x50D) — port and memory allocation issues
- **Device errors** (0x700-0x739) — symbol not found, invalid access, licensing
- **Client errors** (0x740-0x756) — timeout, invalid parameters
- **RTime errors** (0x1000-0x101A) — real-time system failures
- **TCP errors** (0x274C-0x2751) — connection refused, timed out, host down

Each `ReturnCode` implements both `String()` and `Error()`, providing human-readable descriptions like `"0x0710: symbol not found"`.
