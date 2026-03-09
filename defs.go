package ads

import "fmt"

// AMSAddress netid and port of device
type AmsAddress struct {
	NetID [6]byte
	Port  uint16
}

// TransMode transmission mode for notifications
type TransMode uint32

const (
	TransModeNoTransmission  TransMode = 0
	TransModeClientCycle     TransMode = 1
	TransModeClientOnChange  TransMode = 2
	TransModeServerCycle     TransMode = 3
	TransModeServerOnChange  TransMode = 4
	TransModeServerCycle2    TransMode = 5
	TransModeServerOnChange2 TransMode = 6
	TransModeClient1Request  TransMode = 10
)

// String returns a human-readable name for the transmission mode.
func (tm TransMode) String() string {
	switch tm {
	case TransModeNoTransmission:
		return "NoTransmission"
	case TransModeClientCycle:
		return "ClientCycle"
	case TransModeClientOnChange:
		return "ClientOnChange"
	case TransModeServerCycle:
		return "ServerCycle"
	case TransModeServerOnChange:
		return "ServerOnChange"
	case TransModeServerCycle2:
		return "ServerCycle2"
	case TransModeServerOnChange2:
		return "ServerOnChange2"
	case TransModeClient1Request:
		return "Client1Request"
	default:
		return fmt.Sprintf("Unknown(%d)", uint32(tm))
	}
}

// downgradeTransMode converts v2 transmission modes to their v1 equivalents
// for older PLCs (e.g. TwinCAT 2) that silently ignore v2 modes.
func downgradeTransMode(mode TransMode) TransMode {
	switch mode {
	case TransModeServerOnChange2:
		return TransModeServerOnChange
	case TransModeServerCycle2:
		return TransModeServerCycle
	default:
		return mode
	}
}

type AdsState uint16

const (
	AdsStateInvalid      AdsState = 0
	AdsStateIdle         AdsState = 1
	AdsStateReset        AdsState = 2
	AdsStateInit         AdsState = 3
	AdsStateStart        AdsState = 4
	AdsStateRun          AdsState = 5
	AdsStateStop         AdsState = 6
	AdsStateSaveCfg      AdsState = 7
	AdsStateLoadCfg      AdsState = 8
	AdsStatePowerFailure AdsState = 9
	AdsStatePowerGood    AdsState = 10
	AdsStateError        AdsState = 11
	AdsStateShutdown     AdsState = 12
	AdsStateSuspend      AdsState = 13
	AdsStateResume       AdsState = 14
	AdsStateConfig       AdsState = 15 // System Is In Config Mode
	AdsStateReconfig     AdsState = 16 // System Should Restart In Config Mode
	AdsStateMaxStates    AdsState = 255
)

// Port default twincat ports
type Port uint32

const (
	PortLogger    Port = 100
	PortR0Rtime   Port = 200
	PortR0Trace   Port = (PortR0Rtime + 90)
	PortR0Io      Port = 300
	PortR0Sps     Port = 400
	PortR0Nc      Port = 500
	PortR0Isg     Port = 550
	PortR0Pcs     Port = 600
	PortR0Plc     Port = 801
	PortR0PlcRts1 Port = 801
	PortR0PlcRts2 Port = 811
	PortR0PlcRts3 Port = 821
	PortR0PlcRts4 Port = 831
	PortR0PlcTc3  Port = 851
)

// CommandID Ads Command IDS
type CommandID uint16

const (
	CommandIDInValueID CommandID = iota
	CommandIDReadDeviceInfo
	CommandIDRead
	CommandIDWrite
	CommandIDReadState
	CommandIDWriteControl
	CommandIDAddDeviceNotification
	CommandIDDeleteDeviceNotification
	CommandIDDeviceNotification
	CommandIDReadWrite
)

// Group reserved index groups
type Group uint32

const (
	GroupSymbolTab   Group = 0xf000
	GroupSymbolName  Group = 0xf001
	GroupSymbolValue Group = 0xf002

	GroupSymbolHandleByName  Group = 0xF003
	GroupSymbolValueByName   Group = 0xF004
	GroupSymbolValueByHandle Group = 0xF005
	GroupSymbolReleaseHandle Group = 0xF006
	GroupSymbolInfoByName    Group = 0xF007
	GroupSymbolVersion       Group = 0xF008
	GroupSymbolInfoByNameEx  Group = 0xF009

	GroupSymbolDownload       Group = 0xF00A
	GroupSymbolUpload         Group = 0xF00B
	GroupSymbolUploadInfo     Group = 0xF00C
	GroupSymbolDownload2      Group = 0xF00D
	GroupSymbolDataTypeUpload Group = 0xF00E
	GroupSymbolUploadInfo2    Group = 0xF00F

	GroupSymbolNotification Group = 0xf010 // notification of named handle

	GroupSumupRead                     Group = 0xF080
	GroupSumupWrite                    Group = 0xF081
	GroupSumupReadWrite                Group = 0xF082
	GroupSumupReadEx                   Group = 0xF083
	GroupSumupReadEx2                  Group = 0xF084
	GroupSumupAddDeviceNotification    Group = 0xF085
	GroupSumupDeleteDeviceNotification Group = 0xF086

	GroupIoImageRwib   Group = 0xF020 // read/write input byte(s)
	GroupIoImageRwix   Group = 0xF021 // read/write input bit
	GroupIoImageRisize Group = 0xF025 // read input size (in byte)
	GroupIoImageRwob   Group = 0xF030 // read/write output byte(s)
	GroupIoImageRwox   Group = 0xF031 // read/write output bit
	GroupIoImageCleari Group = 0xF040 // write inputs to null
	GroupIoImageClearo Group = 0xF050 // write outputs to null
	GroupIoImageRwiob  Group = 0xF060 // read input and write output byte(s)

	GroupDeviceData Group = 0xF100 // state, name, etc...
)

type Offset uint32

const (
	OffsetDeviceDataAdsState    Offset = 0x0000 // ads state of device
	OffsetDeviceDataDeviceState Offset = 0x0002 // device state
)

// ReturnCode ADS Return codes
type ReturnCode uint32

// ReturnCodeErrorOffset General ADS errors begin at this offset
const ReturnCodeErrorOffset = 0x0700

const (
	ReturnCodeNoErrors ReturnCode = 0x00

	// Global error codes (0x01 - 0x1E)
	ReturnCodeGlobalInternalError       ReturnCode = 0x01
	ReturnCodeGlobalNoRtime             ReturnCode = 0x02
	ReturnCodeGlobalAllocLockedMemError ReturnCode = 0x03
	ReturnCodeGlobalInsertMailboxError  ReturnCode = 0x04
	ReturnCodeGlobalWrongReceiveHmsg    ReturnCode = 0x05
	ReturnCodeGlobalTargetPortNotFound  ReturnCode = 0x06
	ReturnCodeGlobalTargetNotFound      ReturnCode = 0x07
	ReturnCodeGlobalUnknownCommandID    ReturnCode = 0x08
	ReturnCodeGlobalBadTaskID           ReturnCode = 0x09
	ReturnCodeGlobalNoIO                ReturnCode = 0x0A
	ReturnCodeGlobalUnknownAdsCommand   ReturnCode = 0x0B
	ReturnCodeGlobalWin32Error          ReturnCode = 0x0C
	ReturnCodeGlobalPortNotConnected    ReturnCode = 0x0D
	ReturnCodeGlobalInvalidAdsLength    ReturnCode = 0x0E
	ReturnCodeGlobalInvalidAmsNetID     ReturnCode = 0x0F
	ReturnCodeGlobalLowInstallLevel     ReturnCode = 0x10
	ReturnCodeGlobalNoDebugAvailable    ReturnCode = 0x11
	ReturnCodeGlobalPortDisabled        ReturnCode = 0x12
	ReturnCodeGlobalPortAlreadyConn     ReturnCode = 0x13
	ReturnCodeGlobalAdsSyncW32Error     ReturnCode = 0x14
	ReturnCodeGlobalAdsSyncTimeout      ReturnCode = 0x15
	ReturnCodeGlobalAdsSyncAmsError     ReturnCode = 0x16
	ReturnCodeGlobalAdsSyncNoIndexMap   ReturnCode = 0x17
	ReturnCodeGlobalInvalidAdsPort      ReturnCode = 0x18
	ReturnCodeGlobalNoMemory            ReturnCode = 0x19
	ReturnCodeGlobalTcpSendError        ReturnCode = 0x1A
	ReturnCodeGlobalHostUnreachable     ReturnCode = 0x1B
	ReturnCodeGlobalInvalidAmsFragment  ReturnCode = 0x1C
	ReturnCodeGlobalTlsSendError        ReturnCode = 0x1D
	ReturnCodeGlobalAccessDenied        ReturnCode = 0x1E

	// Router error codes (0x500 - 0x50D)
	ReturnCodeRouterNoLockedMemory   ReturnCode = 0x500
	ReturnCodeRouterResizeMemory     ReturnCode = 0x501
	ReturnCodeRouterMailboxFull      ReturnCode = 0x502
	ReturnCodeRouterDebugBoxFull     ReturnCode = 0x503
	ReturnCodeRouterUnknownPortType  ReturnCode = 0x504
	ReturnCodeRouterNotInitialized   ReturnCode = 0x505
	ReturnCodeRouterPortAlreadyInUse ReturnCode = 0x506
	ReturnCodeRouterNotRegistered    ReturnCode = 0x507
	ReturnCodeRouterNoMoreQueues     ReturnCode = 0x508
	ReturnCodeRouterInvalidPort      ReturnCode = 0x509
	ReturnCodeRouterNotActivated     ReturnCode = 0x50A
	ReturnCodeRouterFragmentBoxFull  ReturnCode = 0x50B
	ReturnCodeRouterFragmentTimeout  ReturnCode = 0x50C
	ReturnCodeRouterToBeRemoved      ReturnCode = 0x50D

	// General ADS error codes (0x700 - 0x739)
	ReturnCodeDeviceError                 ReturnCode = (0x00 + ReturnCodeErrorOffset)
	ReturnCodeDeviceServiceNotSupported   ReturnCode = (0x01 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidGroup          ReturnCode = (0x02 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidOffset         ReturnCode = (0x03 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidAccess         ReturnCode = (0x04 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidSize           ReturnCode = (0x05 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidData           ReturnCode = (0x06 + ReturnCodeErrorOffset)
	ReturnCodeDeviceNotReady              ReturnCode = (0x07 + ReturnCodeErrorOffset)
	ReturnCodeDeviceBusy                  ReturnCode = (0x08 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidContext        ReturnCode = (0x09 + ReturnCodeErrorOffset)
	ReturnCodeDeviceNoMemory              ReturnCode = (0x0A + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidParam          ReturnCode = (0x0B + ReturnCodeErrorOffset)
	ReturnCodeDeviceNotFound              ReturnCode = (0x0C + ReturnCodeErrorOffset)
	ReturnCodeDeviceSyntax                ReturnCode = (0x0D + ReturnCodeErrorOffset)
	ReturnCodeDeviceIncompatible          ReturnCode = (0x0E + ReturnCodeErrorOffset)
	ReturnCodeDeviceExists                ReturnCode = (0x0F + ReturnCodeErrorOffset)
	ReturnCodeDeviceSymbolNoFound         ReturnCode = (0x10 + ReturnCodeErrorOffset)
	ReturnCodeDeviceSymbolVersionInvalid  ReturnCode = (0x11 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidState          ReturnCode = (0x12 + ReturnCodeErrorOffset)
	ReturnCodeDeviceTransModeNotSupported ReturnCode = (0x13 + ReturnCodeErrorOffset)
	ReturnCodeDeviceNotifyHandleInvalid   ReturnCode = (0x14 + ReturnCodeErrorOffset)
	ReturnCodeDeviceClientUnknown         ReturnCode = (0x15 + ReturnCodeErrorOffset)
	ReturnCodeDeviceNoMoreHandles         ReturnCode = (0x16 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidWatchSize      ReturnCode = (0x17 + ReturnCodeErrorOffset)
	ReturnCodeDeviceNotInitialized        ReturnCode = (0x18 + ReturnCodeErrorOffset)
	ReturnCodeDeviceTimeout               ReturnCode = (0x19 + ReturnCodeErrorOffset)
	ReturnCodeDeviceNoInterface           ReturnCode = (0x1A + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidInterface      ReturnCode = (0x1B + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidClsID          ReturnCode = (0x1C + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidObjID          ReturnCode = (0x1D + ReturnCodeErrorOffset)
	ReturnCodeDevicePending               ReturnCode = (0x1E + ReturnCodeErrorOffset)
	ReturnCodeDeviceAborted               ReturnCode = (0x1F + ReturnCodeErrorOffset)
	ReturnCodeDeviceWarning               ReturnCode = (0x20 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidArrayIndex     ReturnCode = (0x21 + ReturnCodeErrorOffset)
	ReturnCodeDeviceSymbolNotActive       ReturnCode = (0x22 + ReturnCodeErrorOffset)
	ReturnCodeDeviceAccessDenied          ReturnCode = (0x23 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseNotFound       ReturnCode = (0x24 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseExpired        ReturnCode = (0x25 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseExceeded       ReturnCode = (0x26 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseInvalid        ReturnCode = (0x27 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseSystemID       ReturnCode = (0x28 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseNoTimeLimit    ReturnCode = (0x29 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseFutureIssue    ReturnCode = (0x2A + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseTimeToLong     ReturnCode = (0x2B + ReturnCodeErrorOffset)
	ReturnCodeDeviceException             ReturnCode = (0x2C + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseDuplicated     ReturnCode = (0x2D + ReturnCodeErrorOffset)
	ReturnCodeDeviceSignatureInvalid      ReturnCode = (0x2E + ReturnCodeErrorOffset)
	ReturnCodeDeviceCertificateInvalid    ReturnCode = (0x2F + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseOemNotFound    ReturnCode = (0x30 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseRestricted     ReturnCode = (0x31 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicenseDemoDenied     ReturnCode = (0x32 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidFuncID         ReturnCode = (0x33 + ReturnCodeErrorOffset)
	ReturnCodeDeviceOutOfRange            ReturnCode = (0x34 + ReturnCodeErrorOffset)
	ReturnCodeDeviceInvalidAlignment      ReturnCode = (0x35 + ReturnCodeErrorOffset)
	ReturnCodeDeviceLicensePlatform       ReturnCode = (0x36 + ReturnCodeErrorOffset)
	ReturnCodeDeviceForwardPL             ReturnCode = (0x37 + ReturnCodeErrorOffset)
	ReturnCodeDeviceForwardDL             ReturnCode = (0x38 + ReturnCodeErrorOffset)
	ReturnCodeDeviceForwardRT             ReturnCode = (0x39 + ReturnCodeErrorOffset)

	// Client error codes (0x740 - 0x756)
	ReturnCodeClientError               ReturnCode = (0x40 + ReturnCodeErrorOffset)
	ReturnCodeClientInvalidParameter    ReturnCode = (0x41 + ReturnCodeErrorOffset)
	ReturnCodeClientListEmpty           ReturnCode = (0x42 + ReturnCodeErrorOffset)
	ReturnCodeClientVarUsed             ReturnCode = (0x43 + ReturnCodeErrorOffset)
	ReturnCodeClientDuplicateInvokeID   ReturnCode = (0x44 + ReturnCodeErrorOffset)
	ReturnCodeClientSyncTimeout         ReturnCode = (0x45 + ReturnCodeErrorOffset)
	ReturnCodeClientW32Error            ReturnCode = (0x46 + ReturnCodeErrorOffset)
	ReturnCodeClientTimeoutInvalid      ReturnCode = (0x47 + ReturnCodeErrorOffset)
	ReturnCodeClientPortNotOpen         ReturnCode = (0x48 + ReturnCodeErrorOffset)
	ReturnCodeClientNoAmsAddress        ReturnCode = (0x49 + ReturnCodeErrorOffset)
	ReturnCodeClientSyncInternal        ReturnCode = (0x50 + ReturnCodeErrorOffset)
	ReturnCodeClientAddHash             ReturnCode = (0x51 + ReturnCodeErrorOffset)
	ReturnCodeClientRemoveHash          ReturnCode = (0x52 + ReturnCodeErrorOffset)
	ReturnCodeClientNoMoreSymbols       ReturnCode = (0x53 + ReturnCodeErrorOffset)
	ReturnCodeClientSyncResponseInvalid ReturnCode = (0x54 + ReturnCodeErrorOffset)
	ReturnCodeClientSyncPortLocked      ReturnCode = (0x55 + ReturnCodeErrorOffset)
	ReturnCodeClientRequestCancelled    ReturnCode = (0x56 + ReturnCodeErrorOffset)

	// RTime error codes (0x1000 - 0x101A)
	ReturnCodeRTimeInternal            ReturnCode = 0x1000
	ReturnCodeRTimeBadTimerPeriods     ReturnCode = 0x1001
	ReturnCodeRTimeInvalidTaskPtr      ReturnCode = 0x1002
	ReturnCodeRTimeInvalidStackPtr     ReturnCode = 0x1003
	ReturnCodeRTimePrioExists          ReturnCode = 0x1004
	ReturnCodeRTimeNoMoreTcb           ReturnCode = 0x1005
	ReturnCodeRTimeNoMoreSemas         ReturnCode = 0x1006
	ReturnCodeRTimeNoMoreQueues        ReturnCode = 0x1007
	ReturnCodeRTimeExtIrqAlreadyDef    ReturnCode = 0x100D
	ReturnCodeRTimeExtIrqNotDef        ReturnCode = 0x100E
	ReturnCodeRTimeExtIrqInstallFailed ReturnCode = 0x100F
	ReturnCodeRTimeIrqlNotLessOrEqual  ReturnCode = 0x1010
	ReturnCodeRTimeVmxNotSupported     ReturnCode = 0x1017
	ReturnCodeRTimeVmxDisabled         ReturnCode = 0x1018
	ReturnCodeRTimeVmxControlsMissing  ReturnCode = 0x1019
	ReturnCodeRTimeVmxEnableFails      ReturnCode = 0x101A

	// TCP/Winsock error codes
	ReturnCodeWsaeTimedOut    ReturnCode = 0x274C
	ReturnCodeWsaeConnRefused ReturnCode = 0x274D
	ReturnCodeWsaeHostDown    ReturnCode = 0x2751
)

// returnCodeDescriptions maps return codes to human-readable descriptions.
var returnCodeDescriptions = map[ReturnCode]string{
	ReturnCodeNoErrors: "no error",

	// Global errors
	ReturnCodeGlobalInternalError:       "internal error",
	ReturnCodeGlobalNoRtime:             "no real-time",
	ReturnCodeGlobalAllocLockedMemError: "allocation locked memory error",
	ReturnCodeGlobalInsertMailboxError:  "mailbox full, ADS message could not be sent",
	ReturnCodeGlobalWrongReceiveHmsg:    "wrong receive HMSG",
	ReturnCodeGlobalTargetPortNotFound:  "target port not found, ADS server not started",
	ReturnCodeGlobalTargetNotFound:      "target machine not found, AMS route not found",
	ReturnCodeGlobalUnknownCommandID:    "unknown command ID",
	ReturnCodeGlobalBadTaskID:           "invalid task ID",
	ReturnCodeGlobalNoIO:                "no IO",
	ReturnCodeGlobalUnknownAdsCommand:   "unknown ADS command",
	ReturnCodeGlobalWin32Error:          "Win32 error",
	ReturnCodeGlobalPortNotConnected:    "port not connected",
	ReturnCodeGlobalInvalidAdsLength:    "invalid ADS length",
	ReturnCodeGlobalInvalidAmsNetID:     "invalid AMS Net ID",
	ReturnCodeGlobalLowInstallLevel:     "installation level too low (TwinCAT 2 license error)",
	ReturnCodeGlobalNoDebugAvailable:    "no debugging available",
	ReturnCodeGlobalPortDisabled:        "port disabled, TwinCAT system service not started",
	ReturnCodeGlobalPortAlreadyConn:     "port already connected",
	ReturnCodeGlobalAdsSyncW32Error:     "ADS Sync Win32 error",
	ReturnCodeGlobalAdsSyncTimeout:      "ADS Sync timeout",
	ReturnCodeGlobalAdsSyncAmsError:     "ADS Sync AMS error",
	ReturnCodeGlobalAdsSyncNoIndexMap:   "no index map for ADS Sync available",
	ReturnCodeGlobalInvalidAdsPort:      "invalid ADS port",
	ReturnCodeGlobalNoMemory:            "no memory",
	ReturnCodeGlobalTcpSendError:        "TCP send error",
	ReturnCodeGlobalHostUnreachable:     "host unreachable",
	ReturnCodeGlobalInvalidAmsFragment:  "invalid AMS fragment",
	ReturnCodeGlobalTlsSendError:        "TLS send error, secure ADS connection failed",
	ReturnCodeGlobalAccessDenied:        "access denied, secure ADS access denied",

	// Router errors
	ReturnCodeRouterNoLockedMemory:   "locked memory cannot be allocated",
	ReturnCodeRouterResizeMemory:     "router memory size could not be changed",
	ReturnCodeRouterMailboxFull:      "mailbox full, maximum number of messages reached",
	ReturnCodeRouterDebugBoxFull:     "debug mailbox full",
	ReturnCodeRouterUnknownPortType:  "unknown port type",
	ReturnCodeRouterNotInitialized:   "router is not initialized",
	ReturnCodeRouterPortAlreadyInUse: "port number is already assigned",
	ReturnCodeRouterNotRegistered:    "port not registered",
	ReturnCodeRouterNoMoreQueues:     "maximum number of ports reached",
	ReturnCodeRouterInvalidPort:      "invalid port",
	ReturnCodeRouterNotActivated:     "TwinCAT router not active",
	ReturnCodeRouterFragmentBoxFull:  "fragment mailbox full",
	ReturnCodeRouterFragmentTimeout:  "fragment timeout",
	ReturnCodeRouterToBeRemoved:      "port is being removed",

	// General ADS errors
	ReturnCodeDeviceError:                 "general device error",
	ReturnCodeDeviceServiceNotSupported:   "service not supported by server",
	ReturnCodeDeviceInvalidGroup:          "invalid index group",
	ReturnCodeDeviceInvalidOffset:         "invalid index offset",
	ReturnCodeDeviceInvalidAccess:         "reading/writing not permitted",
	ReturnCodeDeviceInvalidSize:           "parameter size not correct",
	ReturnCodeDeviceInvalidData:           "invalid parameter value(s)",
	ReturnCodeDeviceNotReady:              "device is not in a ready state",
	ReturnCodeDeviceBusy:                  "device is busy",
	ReturnCodeDeviceInvalidContext:        "invalid operating system context (must be in Windows)",
	ReturnCodeDeviceNoMemory:              "out of memory",
	ReturnCodeDeviceInvalidParam:          "invalid parameter value(s)",
	ReturnCodeDeviceNotFound:              "not found (files, ...)",
	ReturnCodeDeviceSyntax:                "syntax error in command or file",
	ReturnCodeDeviceIncompatible:          "objects do not match",
	ReturnCodeDeviceExists:                "object already exists",
	ReturnCodeDeviceSymbolNoFound:         "symbol not found",
	ReturnCodeDeviceSymbolVersionInvalid:  "symbol version invalid, please reload symbols",
	ReturnCodeDeviceInvalidState:          "server is in invalid state",
	ReturnCodeDeviceTransModeNotSupported: "ADS TransMode not supported",
	ReturnCodeDeviceNotifyHandleInvalid:   "notification handle is invalid",
	ReturnCodeDeviceClientUnknown:         "notification client not registered",
	ReturnCodeDeviceNoMoreHandles:         "no more notification handles available",
	ReturnCodeDeviceInvalidWatchSize:      "notification size too large",
	ReturnCodeDeviceNotInitialized:        "device not initialized",
	ReturnCodeDeviceTimeout:               "device has a timeout",
	ReturnCodeDeviceNoInterface:           "query interface failed",
	ReturnCodeDeviceInvalidInterface:      "wrong interface required",
	ReturnCodeDeviceInvalidClsID:          "class ID is invalid",
	ReturnCodeDeviceInvalidObjID:          "object ID is invalid",
	ReturnCodeDevicePending:               "request is pending",
	ReturnCodeDeviceAborted:               "request is aborted",
	ReturnCodeDeviceWarning:               "signal warning",
	ReturnCodeDeviceInvalidArrayIndex:     "invalid array index",
	ReturnCodeDeviceSymbolNotActive:       "symbol not active, release handle and try again",
	ReturnCodeDeviceAccessDenied:          "access denied",
	ReturnCodeDeviceLicenseNotFound:       "missing license",
	ReturnCodeDeviceLicenseExpired:        "license expired",
	ReturnCodeDeviceLicenseExceeded:       "license exceeded",
	ReturnCodeDeviceLicenseInvalid:        "license invalid",
	ReturnCodeDeviceLicenseSystemID:       "license invalid system ID",
	ReturnCodeDeviceLicenseNoTimeLimit:    "license not limited in time",
	ReturnCodeDeviceLicenseFutureIssue:    "license issue time in the future",
	ReturnCodeDeviceLicenseTimeToLong:     "license time period too long",
	ReturnCodeDeviceException:             "exception at system startup",
	ReturnCodeDeviceLicenseDuplicated:     "license file read twice",
	ReturnCodeDeviceSignatureInvalid:      "invalid signature",
	ReturnCodeDeviceCertificateInvalid:    "invalid public key certificate",
	ReturnCodeDeviceLicenseOemNotFound:    "public key not known from OEM",
	ReturnCodeDeviceLicenseRestricted:     "license not valid for this system ID",
	ReturnCodeDeviceLicenseDemoDenied:     "demo license prohibited",
	ReturnCodeDeviceInvalidFuncID:         "invalid function ID",
	ReturnCodeDeviceOutOfRange:            "outside the valid range",
	ReturnCodeDeviceInvalidAlignment:      "invalid alignment",
	ReturnCodeDeviceLicensePlatform:       "invalid platform level",
	ReturnCodeDeviceForwardPL:             "context forward to passive level",
	ReturnCodeDeviceForwardDL:             "context forward to dispatch level",
	ReturnCodeDeviceForwardRT:             "context forward to real-time",

	// Client errors
	ReturnCodeClientError:               "client error",
	ReturnCodeClientInvalidParameter:    "invalid parameter at service call",
	ReturnCodeClientListEmpty:           "polling list is empty",
	ReturnCodeClientVarUsed:             "var connection already in use",
	ReturnCodeClientDuplicateInvokeID:   "invoke ID already in use",
	ReturnCodeClientSyncTimeout:         "timeout elapsed, remote terminal not responding",
	ReturnCodeClientW32Error:            "error in Win32 subsystem",
	ReturnCodeClientTimeoutInvalid:      "invalid client timeout value",
	ReturnCodeClientPortNotOpen:         "ADS port not opened",
	ReturnCodeClientNoAmsAddress:        "no AMS address",
	ReturnCodeClientSyncInternal:        "internal error in ADS sync",
	ReturnCodeClientAddHash:             "hash table overflow",
	ReturnCodeClientRemoveHash:          "key not found in hash table",
	ReturnCodeClientNoMoreSymbols:       "no more symbols in cache",
	ReturnCodeClientSyncResponseInvalid: "invalid response received",
	ReturnCodeClientSyncPortLocked:      "sync port is locked",
	ReturnCodeClientRequestCancelled:    "request was cancelled",

	// RTime errors
	ReturnCodeRTimeInternal:            "internal fatal error in TwinCAT real-time system",
	ReturnCodeRTimeBadTimerPeriods:     "timer value not valid",
	ReturnCodeRTimeInvalidTaskPtr:      "task pointer has invalid value zero",
	ReturnCodeRTimeInvalidStackPtr:     "stack pointer has invalid value zero",
	ReturnCodeRTimePrioExists:          "requested task priority already assigned",
	ReturnCodeRTimeNoMoreTcb:           "no free TCB available (max 64)",
	ReturnCodeRTimeNoMoreSemas:         "no free semaphores available (max 64)",
	ReturnCodeRTimeNoMoreQueues:        "no free queue available (max 64)",
	ReturnCodeRTimeExtIrqAlreadyDef:    "external synchronization interrupt already applied",
	ReturnCodeRTimeExtIrqNotDef:        "no external synchronization interrupt applied",
	ReturnCodeRTimeExtIrqInstallFailed: "external synchronization interrupt application failed",
	ReturnCodeRTimeIrqlNotLessOrEqual:  "service called in wrong context",
	ReturnCodeRTimeVmxNotSupported:     "Intel VT-x extension not supported",
	ReturnCodeRTimeVmxDisabled:         "Intel VT-x extension not enabled in BIOS",
	ReturnCodeRTimeVmxControlsMissing:  "missing feature in Intel VT-x extension",
	ReturnCodeRTimeVmxEnableFails:      "enabling Intel VT-x failed",

	// TCP/Winsock errors
	ReturnCodeWsaeTimedOut:    "connection timed out, host unreachable",
	ReturnCodeWsaeConnRefused: "connection refused, host not responding",
	ReturnCodeWsaeHostDown:    "host is down, connection actively refused",
}

// String returns a human-readable description of the ADS return code.
// For unknown codes, it returns the hex value.
func (rc ReturnCode) String() string {
	if desc, ok := returnCodeDescriptions[rc]; ok {
		return fmt.Sprintf("0x%04X: %s", uint32(rc), desc)
	}
	return fmt.Sprintf("0x%04X: unknown error code", uint32(rc))
}

// Error implements the error interface for ReturnCode, allowing it to be used directly as an error.
func (rc ReturnCode) Error() string {
	return rc.String()
}
