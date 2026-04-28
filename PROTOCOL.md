# ADS/AMS Protocol Specification

Reference documentation for the Beckhoff ADS (Automation Device Specification) protocol, based on the official Beckhoff TwinCAT 3 documentation and empirical testing against TwinCAT 2 and TwinCAT 3 PLCs.

## Table of Contents

- [Overview](#overview)
- [Protocol Stack](#protocol-stack)
- [AMS/TCP Header (6 bytes)](#amstcp-header-6-bytes)
- [AMS Header (32 bytes)](#ams-header-32-bytes)
- [ADS Commands](#ads-commands)
- [Index Groups](#index-groups)
- [Sum Commands (Batch Operations)](#sum-commands-batch-operations)
- [Transmission Modes](#transmission-modes)
- [ADS States](#ads-states)
- [AMS Ports](#ams-ports)
- [Return Codes](#return-codes)
- [TwinCAT 2 vs TwinCAT 3 Differences](#twincat-2-vs-twincat-3-differences)
- [Packet Walkthrough](#packet-walkthrough)
- [Secure ADS (ADS over TLS)](#secure-ads-ads-over-tls)
- [Limits and Recommendations](#limits-and-recommendations)
- [References](#references)

---

## Overview

ADS (Automation Device Specification) is a binary, little-endian protocol developed by Beckhoff for communicating with TwinCAT automation systems. It runs on top of AMS (Automation Message Specification), which handles device addressing and routing.

- **Transport:** TCP port 48898 (standard), port 8016 (Secure ADS over TLS)
- **Route management:** UDP port 48899
- **Byte order:** Little-endian throughout
- **Request/response model:** Each request has an invoke ID echoed in the response
- **Asynchronous notifications:** The PLC pushes data changes to subscribed clients

---

## Protocol Stack

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

## AMS/TCP Header (6 bytes)

| Offset | Size | Field    | Description                                         |
|--------|------|----------|-----------------------------------------------------|
| 0      | 1    | Reserved | Always 0 for normal ADS packets                     |
| 1      | 1    | System   | 0 = normal ADS, non-zero = system/router message    |
| 2      | 4    | Length   | Length of AMS header + ADS data (excludes this header) |

System messages (System > 0) are used for router-level operations such as requesting the local AMS NetID.

---

## AMS Header (32 bytes)

| Offset | Size | Field      | Description                              |
|--------|------|------------|------------------------------------------|
| 0      | 8    | Target     | Target AMS address (6-byte NetID + 2-byte port) |
| 8      | 8    | Source     | Source AMS address (6-byte NetID + 2-byte port) |
| 16     | 2    | Command    | ADS command ID                           |
| 18     | 2    | State      | State flags (4 = request, 5 = response)  |
| 20     | 4    | Length     | Length of ADS data following this header  |
| 24     | 4    | Error Code | AMS error code (0 = no error)            |
| 28     | 4    | Invoke ID  | Unique ID to match responses to requests |

**State Flags (uint16):**

| Value | Meaning                 |
|-------|-------------------------|
| 0x0004 | ADS request (expects response) |
| 0x0005 | ADS response            |

### AMS Address (8 bytes)

```text
NetID (6 bytes) + Port (2 bytes)
```

The NetID is typically displayed as `x.x.x.x.x.x` (e.g., `192.168.1.100.1.1`). By convention, the first 4 bytes often match the device IP and the last 2 bytes are `1.1`.

---

## ADS Commands

| ID | Hex    | Command                      | Description                                       |
|----|--------|------------------------------|---------------------------------------------------|
| 0  | 0x0000 | Invalid                      | Not used                                          |
| 1  | 0x0001 | ReadDeviceInfo               | Read device name and version                      |
| 2  | 0x0002 | Read                         | Read data by index group and offset               |
| 3  | 0x0003 | Write                        | Write data by index group and offset              |
| 4  | 0x0004 | ReadState                    | Read ADS and device state                         |
| 5  | 0x0005 | WriteControl                 | Change ADS/device state                           |
| 6  | 0x0006 | AddDeviceNotification        | Subscribe to data change notifications            |
| 7  | 0x0007 | DeleteDeviceNotification     | Unsubscribe from notifications                    |
| 8  | 0x0008 | DeviceNotification           | Notification data pushed from PLC to client       |
| 9  | 0x0009 | ReadWrite                    | Combined read and write in a single round-trip    |

### ReadDeviceInfo (Command 1)

**Request:** No ADS payload.

**Response:**

| Size | Field | Description                  |
|------|-------|------------------------------|
| 4    | Error | ADS return code              |
| 1    | Major | Major version number         |
| 1    | Minor | Minor version number         |
| 2    | Build | Build number                 |
| 16   | Name  | Device name (null-terminated)|

### Read (Command 2)

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

Only a 4-byte return code. No data payload.

### ReadState (Command 4)

**Request:** No ADS payload.

**Response:**

| Size | Field       | Description         |
|------|-------------|---------------------|
| 4    | Error       | ADS return code     |
| 2    | AdsState    | Current ADS state   |
| 2    | DeviceState | Current device state|

### WriteControl (Command 5)

**Request:**

| Size | Field       | Description                  |
|------|-------------|------------------------------|
| 2    | AdsState    | Requested ADS state          |
| 2    | DeviceState | Requested device state       |
| 4    | Length      | Length of additional data     |
| n    | Data        | Additional data (optional)   |

**Response:**

| Size | Field | Description     |
|------|-------|-----------------|
| 4    | Error | ADS return code |

### AddDeviceNotification (Command 6)

**Request (40 bytes):**

| Size | Field            | Description                              |
|------|------------------|------------------------------------------|
| 4    | Group            | Index group                              |
| 4    | Offset           | Index offset                             |
| 4    | Length            | Data length to monitor                   |
| 4    | TransmissionMode | When to send notifications               |
| 4    | MaxDelay         | Maximum delay before sending (100ns units) |
| 4    | CycleTime        | Check interval (100ns units)             |
| 16   | Reserved         | Must be zero                             |

**Response:**

| Size | Field  | Description         |
|------|--------|---------------------|
| 4    | Error  | ADS return code     |
| 4    | Handle | Notification handle |

### DeleteDeviceNotification (Command 7)

**Request:**

| Size | Field  | Description         |
|------|--------|---------------------|
| 4    | Handle | Notification handle |

**Response:**

| Size | Field | Description     |
|------|-------|-----------------|
| 4    | Error | ADS return code |

### DeviceNotification (Command 8)

Pushed from PLC to client. Not a request/response — sent asynchronously.

```text
NotificationStream:
  Length    (4 bytes) — total data length
  Stamps   (4 bytes) — number of stamp headers

For each stamp:
  StampHeader:
    Timestamp (8 bytes) — Windows FILETIME (100ns since 1601-01-01)
    Samples   (4 bytes) — number of samples in this stamp

  For each sample:
    NotificationSample:
      Handle (4 bytes) — notification handle
      Size   (4 bytes) — data size
      Data   (n bytes) — the notification payload
```

Timestamp conversion to Unix: `unixSeconds = (filetime / 10000000) - 11644473600`

### ReadWrite (Command 9)

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

---

## Index Groups

### Symbol System

| Group  | Name                    | Purpose                                       |
|--------|-------------------------|-----------------------------------------------|
| 0xF000 | SymbolTab               | Symbol table                                  |
| 0xF001 | SymbolName              | Symbol by name                                |
| 0xF002 | SymbolValue             | Symbol by value                               |
| 0xF003 | SymbolHandleByName      | Get a handle for a symbol name                |
| 0xF004 | SymbolValueByName       | Read symbol value by name (one-shot)          |
| 0xF005 | SymbolValueByHandle     | Read/write symbol value using handle          |
| 0xF006 | SymbolReleaseHandle     | Release a previously acquired handle          |
| 0xF007 | SymbolInfoByName        | Get symbol metadata by name                   |
| 0xF008 | SymbolVersion           | Read symbol version (changes on PLC download) |
| 0xF009 | SymbolInfoByNameEx      | Get extended symbol info by name              |
| 0xF00A | SymbolDownload          | Symbol download                               |
| 0xF00B | SymbolUpload            | Download full symbol table from PLC           |
| 0xF00C | SymbolUploadInfo        | Get symbol table size info                    |
| 0xF00D | SymbolDownload2         | Symbol download v2                            |
| 0xF00E | SymbolDataTypeUpload    | Download all datatype definitions             |
| 0xF00F | SymbolUploadInfo2       | Extended symbol upload info (TC3 only)        |
| 0xF010 | SymbolNotification      | Notification of named handle                  |

### I/O Image

| Group  | Name           | Purpose                              |
|--------|----------------|--------------------------------------|
| 0xF020 | IoImageRwib    | Read/write input byte(s)            |
| 0xF021 | IoImageRwix    | Read/write input bit                |
| 0xF025 | IoImageRisize  | Read input size (in bytes)          |
| 0xF030 | IoImageRwob    | Read/write output byte(s)           |
| 0xF031 | IoImageRwox    | Read/write output bit               |
| 0xF040 | IoImageCleari  | Write inputs to null                |
| 0xF050 | IoImageClearo  | Write outputs to null               |
| 0xF060 | IoImageRwiob   | Read input and write output byte(s) |

### Sum Commands

| Group  | Name                          | Purpose                              |
|--------|-------------------------------|--------------------------------------|
| 0xF080 | SumupRead                     | Batch read (basic)                   |
| 0xF081 | SumupWrite                    | Batch write                          |
| 0xF082 | SumupReadWrite                | Batch read+write                     |
| 0xF083 | SumupReadEx                   | Batch read with per-item errors      |
| 0xF084 | SumupReadEx2                  | Batch read v2 (TC3 only)            |
| 0xF085 | SumupAddDeviceNotification    | Batch add notifications (TC3 only)  |
| 0xF086 | SumupDeleteDeviceNotification | Batch delete notifications (TC3 only)|

### Other

| Group  | Name       | Purpose                    |
|--------|------------|----------------------------|
| 0xF100 | DeviceData | State, name, etc.          |

---

## Sum Commands (Batch Operations)

Sum commands combine multiple operations into a single ADS ReadWrite (Command 9) call. The index group identifies the sum command type. The offset field carries the sub-command count `N`.

### 0xF080 — SumRead (Basic)

**ReadWrite parameters:**
- Group: `0xF080`, Offset: `N`
- ReadLength: `N × 4 + sum(requested lengths)`

**Write data:** `N × 12 bytes` — per item: IndexGroup(4) + IndexOffset(4) + ReadLength(4)

**Read data (response):**
```text
[N × Error(4)]  [Data(0) .. Data(N-1)]
```

No per-item read lengths in response. Client must use originally requested lengths to split data.

### 0xF081 — SumWrite

**ReadWrite parameters:**
- Group: `0xF081`, Offset: `N`
- ReadLength: `N × 4`

**Write data:** Headers `N × 12 bytes` [IndexGroup(4) + IndexOffset(4) + WriteLength(4)] followed by concatenated write data.

**Read data (response):**
```text
[N × Error(4)]
```

### 0xF082 — SumReadWrite

**ReadWrite parameters:**
- Group: `0xF082`, Offset: `N`
- ReadLength: `N × 8 + sum(read lengths)`

**Write data:** Headers `N × 16 bytes` [IndexGroup(4) + IndexOffset(4) + ReadLength(4) + WriteLength(4)] followed by concatenated write data.

**Read data (response):**
```text
[N × (Error(4), ReadLength(4))]  [ReadData(0) .. ReadData(N-1)]
```

### 0xF083 — SumReadEx

Extended batch read with per-item error codes and actual read lengths.

**ReadWrite parameters:**
- Group: `0xF083`, Offset: `N`
- ReadLength: `N × 8 + sum(requested lengths)`

**Write data:** `N × 12 bytes` — per item: IndexGroup(4) + IndexOffset(4) + ReadLength(4)

**Read data (response):**
```text
[N × (Error(4), ReadLength(4))]  [Data(0) .. Data(N-1)]
```

> **Documentation note:** The official Beckhoff PDF (page 36) shows 0xF083 with the same response format as 0xF080 (separate error array, no per-item lengths). Empirical testing on both TwinCAT 2 and TwinCAT 3 confirms 0xF083 actually returns the interleaved `[N × (error, length)][data]` format, identical to 0xF084.

### 0xF084 — SumReadEx2

Same request and response format as 0xF083. The official documentation (page 39) explicitly shows the interleaved format for this command:

```text
Error(1), ReadLen(1), ..., Error(N), ReadLen(N), ReadData(1..N)
```

**TC3 only.** Returns `0x0701` (service not supported) on TwinCAT 2.

### 0xF085 — SumAddDeviceNotification

**ReadWrite parameters:**
- Group: `0xF085`, Offset: `N`
- ReadLength: `N × 8`

**Write data:** `N × 40 bytes` per item — matches the full AddDeviceNotification request structure:

| Size | Field            |
|------|------------------|
| 4    | IndexGroup       |
| 4    | IndexOffset      |
| 4    | Length           |
| 4    | TransmissionMode |
| 4    | MaxDelay (100ns) |
| 4    | CycleTime (100ns)|
| 16   | Reserved (zero)  |

**Read data (response):**
```text
[N × (Error(4), Handle(4))]
```

**TC3 only.** Not supported on TwinCAT 2.

### 0xF086 — SumDeleteDeviceNotification

**ReadWrite parameters:**
- Group: `0xF086`, Offset: `N`
- ReadLength: `N × 4`

**Write data:** `N × 4 bytes` — one Handle(4) per item.

**Read data (response):**
```text
[N × Error(4)]
```

**TC3 only.** Not supported on TwinCAT 2.

---

## Transmission Modes

Used in AddDeviceNotification (Command 6) and SumAddDeviceNotification (0xF085).

| Value | Name             | Description                                   |
|-------|------------------|-----------------------------------------------|
| 0     | NoTransmission   | No notifications                              |
| 1     | ClientCycle      | Client-side cyclic check (deprecated)         |
| 2     | ClientOnChange   | Client-side on-change (deprecated)            |
| 3     | ServerCycle      | Server sends at fixed intervals               |
| 4     | ServerOnChange   | Server sends only when value changes          |
| 5     | ServerCycle2     | ServerCycle with timestamp (TC3 only)         |
| 6     | ServerOnChange2  | ServerOnChange with timestamp (TC3 only)      |
| 10    | Client1Request   | Single notification then auto-delete          |

The v2 modes (5, 6) include higher-resolution timestamps and were introduced in TwinCAT 3. TwinCAT 2 **silently ignores** v2 modes — notifications will never fire, without returning an error.

---

## ADS States

| Value | State        | Description               |
|-------|-------------|---------------------------|
| 0     | Invalid     | Invalid state             |
| 1     | Idle        | Idle                      |
| 2     | Reset       | Resetting                 |
| 3     | Init        | Initializing              |
| 4     | Start       | Starting                  |
| 5     | Run         | Running (normal operation)|
| 6     | Stop        | Stopped                   |
| 7     | SaveCfg     | Saving configuration      |
| 8     | LoadCfg     | Loading configuration     |
| 9     | PowerFailure| Power failure detected    |
| 10    | PowerGood   | Power restored            |
| 11    | Error       | Error state               |
| 12    | Shutdown    | Shutting down             |
| 13    | Suspend     | Suspended                 |
| 14    | Resume      | Resuming                  |
| 15    | Config      | Config mode               |
| 16    | Reconfig    | Restart in config mode    |

---

## AMS Ports

| Port | Service                          |
|------|----------------------------------|
| 100  | Logger                           |
| 200  | Real-time system                 |
| 290  | Real-time trace                  |
| 300  | I/O                              |
| 400  | SPS (TwinCAT 2 legacy)          |
| 500  | NC                               |
| 550  | ISG (Interpolation)             |
| 600  | PCS                              |
| 801  | PLC Runtime 1 (TC2 & TC3)       |
| 811  | PLC Runtime 2                    |
| 821  | PLC Runtime 3                    |
| 831  | PLC Runtime 4                    |
| 851  | PLC Runtime 1 (TC3 preferred)   |

TwinCAT 2 uses ports 801-831. TwinCAT 3 introduced port 851+ but also supports the legacy range.

---

## Return Codes

Every ADS response includes a ReturnCode (UINT32). Value `0x0000` means success.

Errors can occur at two levels:
- **AMS level** — the Error Code field in the AMS header (routing failures, target not found)
- **ADS level** — the Result/Error field in the command response (invalid group, symbol not found)

### Global Errors (0x01 — 0x1E)

| Code   | Name                        | Description                                          |
|--------|-----------------------------|------------------------------------------------------|
| 0x0001 | InternalError               | Internal error                                       |
| 0x0002 | NoRtime                     | No real-time                                         |
| 0x0003 | AllocLockedMemError         | Allocation locked memory error                       |
| 0x0004 | InsertMailboxError          | Mailbox full, ADS message could not be sent          |
| 0x0005 | WrongReceiveHmsg           | Wrong receive HMSG                                   |
| 0x0006 | TargetPortNotFound          | Target port not found, ADS server not started        |
| 0x0007 | TargetNotFound              | Target machine not found, AMS route not found        |
| 0x0008 | UnknownCommandID            | Unknown command ID                                   |
| 0x0009 | BadTaskID                   | Invalid task ID                                      |
| 0x000A | NoIO                        | No IO                                                |
| 0x000B | UnknownAdsCommand           | Unknown ADS command                                  |
| 0x000C | Win32Error                  | Win32 error                                          |
| 0x000D | PortNotConnected            | Port not connected                                   |
| 0x000E | InvalidAdsLength            | Invalid ADS length                                   |
| 0x000F | InvalidAmsNetID             | Invalid AMS Net ID                                   |
| 0x0010 | LowInstallLevel             | Installation level too low (TC2 license error)       |
| 0x0011 | NoDebugAvailable            | No debugging available                               |
| 0x0012 | PortDisabled                | Port disabled, system service not started            |
| 0x0013 | PortAlreadyConnected        | Port already connected                               |
| 0x0014 | AdsSyncW32Error             | ADS Sync Win32 error                                 |
| 0x0015 | AdsSyncTimeout              | ADS Sync timeout                                     |
| 0x0016 | AdsSyncAmsError             | ADS Sync AMS error                                   |
| 0x0017 | AdsSyncNoIndexMap           | No index map for ADS Sync available                  |
| 0x0018 | InvalidAdsPort              | Invalid ADS port                                     |
| 0x0019 | NoMemory                    | No memory                                            |
| 0x001A | TcpSendError                | TCP send error                                       |
| 0x001B | HostUnreachable             | Host unreachable                                     |
| 0x001C | InvalidAmsFragment          | Invalid AMS fragment                                 |
| 0x001D | TlsSendError                | TLS send error, secure ADS connection failed         |
| 0x001E | AccessDenied                | Access denied, secure ADS access denied              |

### Router Errors (0x0500 — 0x050D)

| Code   | Name                  | Description                                    |
|--------|-----------------------|------------------------------------------------|
| 0x0500 | NoLockedMemory        | Locked memory cannot be allocated              |
| 0x0501 | ResizeMemory          | Router memory size could not be changed        |
| 0x0502 | MailboxFull           | Maximum number of messages reached             |
| 0x0503 | DebugBoxFull          | Debug mailbox full                             |
| 0x0504 | UnknownPortType       | Unknown port type                              |
| 0x0505 | NotInitialized        | Router is not initialized                      |
| 0x0506 | PortAlreadyInUse      | Port number is already assigned                |
| 0x0507 | NotRegistered         | Port not registered                            |
| 0x0508 | NoMoreQueues          | Maximum number of ports reached                |
| 0x0509 | InvalidPort           | Invalid port                                   |
| 0x050A | NotActivated          | TwinCAT router not active                      |
| 0x050B | FragmentBoxFull       | Fragment mailbox full                          |
| 0x050C | FragmentTimeout       | Fragment timeout                               |
| 0x050D | ToBeRemoved           | Port is being removed                          |

### Device/ADS Errors (0x0700 — 0x0739)

| Code   | Name                       | Description                                    |
|--------|----------------------------|------------------------------------------------|
| 0x0700 | Error                      | General device error                           |
| 0x0701 | ServiceNotSupported        | Service not supported by server                |
| 0x0702 | InvalidGroup               | Invalid index group                            |
| 0x0703 | InvalidOffset              | Invalid index offset                           |
| 0x0704 | InvalidAccess              | Reading/writing not permitted                  |
| 0x0705 | InvalidSize                | Parameter size not correct                     |
| 0x0706 | InvalidData                | Invalid parameter value(s)                     |
| 0x0707 | NotReady                   | Device is not in a ready state                 |
| 0x0708 | Busy                       | Device is busy                                 |
| 0x0709 | InvalidContext             | Invalid operating system context               |
| 0x070A | NoMemory                   | Out of memory                                  |
| 0x070B | InvalidParam               | Invalid parameter value(s)                     |
| 0x070C | NotFound                   | Not found (files, ...)                         |
| 0x070D | Syntax                     | Syntax error in command or file                |
| 0x070E | Incompatible               | Objects do not match                           |
| 0x070F | Exists                     | Object already exists                          |
| 0x0710 | SymbolNotFound             | Symbol not found                               |
| 0x0711 | SymbolVersionInvalid       | Symbol version invalid, reload symbols         |
| 0x0712 | InvalidState               | Server is in invalid state                     |
| 0x0713 | TransModeNotSupported      | ADS TransMode not supported                    |
| 0x0714 | NotifyHandleInvalid        | Notification handle is invalid                 |
| 0x0715 | ClientUnknown              | Notification client not registered             |
| 0x0716 | NoMoreHandles              | No more notification handles available         |
| 0x0717 | InvalidWatchSize           | Notification size too large                    |
| 0x0718 | NotInitialized             | Device not initialized                         |
| 0x0719 | Timeout                    | Device has a timeout                           |
| 0x071A | NoInterface                | Query interface failed                         |
| 0x071B | InvalidInterface           | Wrong interface required                       |
| 0x071C | InvalidClsID               | Class ID is invalid                            |
| 0x071D | InvalidObjID               | Object ID is invalid                           |
| 0x071E | Pending                    | Request is pending                             |
| 0x071F | Aborted                    | Request is aborted                             |
| 0x0720 | Warning                    | Signal warning                                 |
| 0x0721 | InvalidArrayIndex          | Invalid array index                            |
| 0x0722 | SymbolNotActive            | Symbol not active, release handle and retry    |
| 0x0723 | AccessDenied               | Access denied                                  |
| 0x0724 | LicenseNotFound            | Missing license                                |
| 0x0725 | LicenseExpired             | License expired                                |
| 0x0726 | LicenseExceeded            | License exceeded                               |
| 0x0727 | LicenseInvalid             | License invalid                                |
| 0x0728 | LicenseSystemID            | License invalid system ID                      |
| 0x0729 | LicenseNoTimeLimit         | License not limited in time                    |
| 0x072A | LicenseFutureIssue         | License issue time in the future               |
| 0x072B | LicenseTimeToLong          | License time period too long                   |
| 0x072C | Exception                  | Exception at system startup                    |
| 0x072D | LicenseDuplicated          | License file read twice                        |
| 0x072E | SignatureInvalid           | Invalid signature                              |
| 0x072F | CertificateInvalid         | Invalid public key certificate                 |
| 0x0730 | LicenseOemNotFound         | Public key not known from OEM                  |
| 0x0731 | LicenseRestricted          | License not valid for this system ID           |
| 0x0732 | LicenseDemoDenied          | Demo license prohibited                        |
| 0x0733 | InvalidFuncID              | Invalid function ID                            |
| 0x0734 | OutOfRange                 | Outside the valid range                        |
| 0x0735 | InvalidAlignment           | Invalid alignment                              |
| 0x0736 | LicensePlatform            | Invalid platform level                         |
| 0x0737 | ForwardPL                  | Context forward to passive level               |
| 0x0738 | ForwardDL                  | Context forward to dispatch level              |
| 0x0739 | ForwardRT                  | Context forward to real-time                   |

### Client Errors (0x0740 — 0x0756)

| Code   | Name                       | Description                                    |
|--------|----------------------------|------------------------------------------------|
| 0x0740 | Error                      | Client error                                   |
| 0x0741 | InvalidParameter           | Invalid parameter at service call              |
| 0x0742 | ListEmpty                  | Polling list is empty                          |
| 0x0743 | VarUsed                    | Var connection already in use                  |
| 0x0744 | DuplicateInvokeID          | Invoke ID already in use                       |
| 0x0745 | SyncTimeout                | Timeout elapsed, remote not responding         |
| 0x0746 | W32Error                   | Error in Win32 subsystem                       |
| 0x0747 | TimeoutInvalid             | Invalid client timeout value                   |
| 0x0748 | PortNotOpen                | ADS port not opened                            |
| 0x0749 | NoAmsAddress               | No AMS address                                 |
| 0x0750 | SyncInternal               | Internal error in ADS sync                     |
| 0x0751 | AddHash                    | Hash table overflow                            |
| 0x0752 | RemoveHash                 | Key not found in hash table                    |
| 0x0753 | NoMoreSymbols              | No more symbols in cache                       |
| 0x0754 | SyncResponseInvalid        | Invalid response received                      |
| 0x0755 | SyncPortLocked             | Sync port is locked                            |
| 0x0756 | RequestCancelled           | Request was cancelled                          |

### RTime Errors (0x1000 — 0x101A)

| Code   | Name                       | Description                                    |
|--------|----------------------------|------------------------------------------------|
| 0x1000 | Internal                   | Internal fatal error in real-time system       |
| 0x1001 | BadTimerPeriods            | Timer value not valid                          |
| 0x1002 | InvalidTaskPtr             | Task pointer has invalid value zero            |
| 0x1003 | InvalidStackPtr            | Stack pointer has invalid value zero           |
| 0x1004 | PrioExists                 | Requested task priority already assigned       |
| 0x1005 | NoMoreTcb                  | No free TCB available (max 64)                 |
| 0x1006 | NoMoreSemas                | No free semaphores available (max 64)          |
| 0x1007 | NoMoreQueues               | No free queue available (max 64)               |
| 0x100D | ExtIrqAlreadyDef           | External sync interrupt already applied        |
| 0x100E | ExtIrqNotDef               | No external sync interrupt applied             |
| 0x100F | ExtIrqInstallFailed        | External sync interrupt application failed     |
| 0x1010 | IrqlNotLessOrEqual         | Service called in wrong context                |
| 0x1017 | VmxNotSupported            | Intel VT-x extension not supported             |
| 0x1018 | VmxDisabled                | Intel VT-x extension not enabled in BIOS       |
| 0x1019 | VmxControlsMissing         | Missing feature in Intel VT-x extension        |
| 0x101A | VmxEnableFails             | Enabling Intel VT-x failed                     |

### TCP/Winsock Errors

| Code   | Name            | Description                                  |
|--------|-----------------|----------------------------------------------|
| 0x274C | TimedOut        | Connection timed out, host unreachable       |
| 0x274D | ConnRefused     | Connection refused, host not responding      |
| 0x2751 | HostDown        | Host is down, connection actively refused    |

---

## TwinCAT 2 vs TwinCAT 3 Differences

### Sum Command Support

| Command                    | Group  | TC2 | TC3 |
|----------------------------|--------|-----|-----|
| SumRead (basic)            | 0xF080 | Yes | Yes |
| SumWrite                   | 0xF081 | Yes | Yes |
| SumReadWrite               | 0xF082 | Yes | Yes |
| SumReadEx                  | 0xF083 | Yes | Yes |
| SumReadEx2                 | 0xF084 | No  | Yes |
| SumAddDeviceNotification   | 0xF085 | No  | Yes |
| SumDeleteDeviceNotification| 0xF086 | No  | Yes |

Unsupported commands return `0x0701` (service not supported).

### Transmission Modes

| Mode             | TC2                | TC3     |
|------------------|--------------------|---------|
| ServerCycle (3)  | Works              | Works   |
| ServerOnChange (4)| Works             | Works   |
| ServerCycle2 (5) | Silently ignored   | Works   |
| ServerOnChange2 (6)| Silently ignored | Works   |

TC2 does not return an error for v2 modes — notifications simply never fire.

### Symbol System

| Feature                    | TC2               | TC3                  |
|----------------------------|-------------------|----------------------|
| SymbolUploadInfo2 (0xF00F) | Not available     | Available            |
| SymbolUploadInfo (0xF00C)  | Available         | Available            |
| Enum types                 | Flattened to base INT/DINT | Full enum metadata |
| Chunked symbol download    | May not support   | Supported            |

### Ports

| Runtime   | TC2  | TC3           |
|-----------|------|---------------|
| PLC RT 1  | 801  | 851 (preferred), 801 also works |
| PLC RT 2  | 811  | 811           |
| PLC RT 3  | 821  | 821           |
| PLC RT 4  | 831  | 831           |

---

## Packet Walkthrough

This section shows a complete ADS Read request and response at the byte level, illustrating how a client reads 4 bytes from a PLC variable.

### Scenario

- Client IP: `192.168.1.10`, AMS NetID: `192.168.1.10.1.1`, AMS port: `10500`
- PLC IP: `192.168.1.100`, AMS NetID: `192.168.1.100.1.1`, AMS port: `851`
- Operation: Read 4 bytes from index group `0xF005` (SymbolValueByHandle), offset `0x0000002A` (handle 42)
- Invoke ID: `1`

### Step 1: TCP Connection

Client opens a TCP connection to `192.168.1.100:48898`.

> Before this, a route must exist on the PLC mapping the client's NetID to its IP. Routes can be pre-configured or added via UDP port 48899.

### Step 2: ADS Read Request

The client sends a single TCP packet containing the AMS/TCP header, AMS header, and ADS Read command data.

```text
Complete packet (50 bytes):

AMS/TCP Header (6 bytes):
  00 00             Reserved + System (normal ADS packet)
  2C 00 00 00       Length = 44 (32-byte AMS header + 12-byte ADS data)

AMS Header (32 bytes):
  C0 A8 01 64 01 01 Target NetID = 192.168.1.100.1.1
  53 03             Target Port = 851 (0x0353)
  C0 A8 01 0A 01 01 Source NetID = 192.168.1.10.1.1
  14 29             Source Port = 10500 (0x2914)
  02 00             Command = 2 (Read)
  04 00             State = 4 (request)
  0C 00 00 00       ADS Data Length = 12
  00 00 00 00       Error Code = 0
  01 00 00 00       Invoke ID = 1

ADS Data — Read Request (12 bytes):
  05 F0 00 00       Index Group = 0xF005 (SymbolValueByHandle)
  2A 00 00 00       Index Offset = 42 (handle)
  04 00 00 00       Read Length = 4
```

### Step 3: ADS Read Response

The PLC responds with the requested data. In this example, the variable holds the DINT value `1234` (0x000004D2).

```text
Complete packet (50 bytes):

AMS/TCP Header (6 bytes):
  00 00             Reserved + System
  2C 00 00 00       Length = 44 (32-byte AMS header + 12-byte ADS data)

AMS Header (32 bytes):
  C0 A8 01 0A 01 01 Target NetID = 192.168.1.10.1.1  (swapped — response goes back)
  14 29             Target Port = 10500
  C0 A8 01 64 01 01 Source NetID = 192.168.1.100.1.1
  53 03             Source Port = 851
  02 00             Command = 2 (Read)
  05 00             State = 5 (response)
  0C 00 00 00       ADS Data Length = 12
  00 00 00 00       Error Code = 0 (AMS-level OK)
  01 00 00 00       Invoke ID = 1 (matches request)

ADS Data — Read Response (12 bytes):
  00 00 00 00       Result = 0x0000 (ADS-level OK)
  04 00 00 00       Data Length = 4
  D2 04 00 00       Data = 1234 (DINT, little-endian)
```

### Step 4: Matching Request to Response

The client matches the response to the request using the **Invoke ID** (1). Multiple requests can be in flight simultaneously — each gets a unique Invoke ID. The response's State field changes from `0x0004` (request) to `0x0005` (response), and the Source/Target addresses are swapped.

### Error Levels

Errors can occur at two independent levels:

1. **AMS Error Code** (offset 24 in AMS header): Routing or transport failures. If non-zero, the ADS data may be absent or invalid.
2. **ADS Result** (first 4 bytes of response data): Command-level errors like "symbol not found" or "invalid group". The AMS Error Code is 0 in this case — the packet was delivered, but the command failed.

### Full Session: Connect → Read Symbol → Disconnect

A typical session to read a PLC variable by name:

```text
1. TCP connect to PLC:48898
        │
2. ReadWrite (cmd 9) to 0xF003 (SymbolHandleByName)
   Write: "MAIN.myVar\0" (symbol name, null-terminated)
   Read:  4 bytes → handle (e.g., 42)
        │
3. Read (cmd 2) to 0xF005 (SymbolValueByHandle)
   Group=0xF005, Offset=42 (handle), Length=4
   Read:  4 bytes → raw value (e.g., 1234)
        │
4. Write (cmd 3) to 0xF006 (SymbolReleaseHandle)
   Group=0xF006, Offset=0, Data=handle (42)
   Response: 4-byte return code
        │
5. TCP close
```

Each step is one ADS request/response pair. Step 2 uses ReadWrite because it sends data (the name) and receives data (the handle) in one round-trip. Step 3 is a pure Read. Step 4 is a pure Write.

---

## Secure ADS (ADS over TLS)

Introduced in TwinCAT 3.1 Build 4024.0. Uses **TLS 1.2 with mutual authentication (mTLS)** on TCP port **8016**.

### Key Differences from Standard ADS

- Uses port 8016 instead of 48898
- **Omits the 6-byte AMS/TCP framing header** inside the TLS tunnel (TLS record layer provides framing)
- Adds a TlsConnectInfo handshake after TLS establishment

### Authentication Methods

1. **Self-Signed Certificates (SSC)** — auto-generated, TOFU with SHA-256 fingerprint pinning
2. **Pre-Shared Keys (PSK)** — TLS-PSK, no certificates needed, no expiration
3. **Customer CA Certificates** — shared CA trust, most flexible for large deployments

### TLS Error Codes

- `0x001D` — TLS send error, secure ADS connection failed
- `0x001E` — Access denied, secure ADS access denied

---

## Limits and Recommendations

| Limit | Value | Source |
|-------|-------|--------|
| Max sub-commands per sum request | 500 | Beckhoff documentation |
| Max notifications per device | 550 | Beckhoff recommendation |
| AMS router response buffer | 2 MB (default) | Configurable via `ADS.Config::RESPONSE_SIZE_LIMIT` |
| AddDeviceNotification request size | 40 bytes | Fixed by protocol |
| AMS/TCP header | 6 bytes | Fixed |
| AMS header | 32 bytes | Fixed |

### Route Registration

UDP port 48899 is used for route management. Packet format:
- Header: cookie `0x71146603` + invoke ID + service ID + AMS address + tag count
- Tags: NetID, password, computer name, route name, username

---

## References

- [Beckhoff InfoSys: ADS/AMS Specification](https://infosys.beckhoff.com/content/1033/tc3_ads_intro/index.html)
- [Beckhoff InfoSys: TwinCAT 3 Basics](https://infosys.beckhoff.com/content/1033/tc3_grundlagen/index.html)
- [Beckhoff TwinCAT 3 Basics PDF](https://download.beckhoff.com/download/document/automation/twincat3/TwinCAT_3_Basics_EN.pdf) — pages 34-39 cover sum commands
- [Beckhoff InfoSys: ADS Return Codes](https://infosys.beckhoff.com/content/1033/tc3_ads_intro/374277003.html)
- [Beckhoff InfoSys: Secure ADS](https://infosys.beckhoff.com/content/1033/tc3_grundlagen/6798095243.html)
- [Beckhoff Secure ADS Manual (PDF)](https://download.beckhoff.com/download/document/automation/twincat3/Secure_ADS_EN.pdf)
