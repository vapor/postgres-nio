import NIOCore
//import struct Foundation.Data


/// A protocol to implement for all associated value in the `PSQLBackendMessage` enum
protocol PSQLMessagePayloadDecodable {
    
    /// Decodes the associated value for a `PSQLBackendMessage` from the given `ByteBuffer`.
    ///
    /// When the decoding is done all bytes in the given `ByteBuffer` must be consumed.
    /// `buffer.readableBytes` must be `0`. In case of an error a `PartialDecodingError`
    /// must be thrown.
    ///
    /// - Parameter buffer: The `ByteBuffer` to read the message from. When done the `ByteBuffer`
    ///                     must be fully consumed.
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

/// A wire message that is created by a Postgres server to be consumed by Postgres client.
///
/// All messages are defined in the official Postgres Documentation in the section
/// [Frontend/Backend Protocol – Message Formats](https://www.postgresql.org/docs/13/protocol-message-formats.html)
enum PSQLBackendMessage: Equatable {
    
    typealias PayloadDecodable = PSQLMessagePayloadDecodable
    
    case authentication(Authentication)
    case backendKeyData(BackendKeyData)
    case bindComplete
    case closeComplete
    case commandComplete(String)
    case dataRow(DataRow)
    case emptyQueryResponse
    case error(ErrorResponse)
    case noData
    case notice(NoticeResponse)
    case notification(NotificationResponse)
    case parameterDescription(ParameterDescription)
    case parameterStatus(ParameterStatus)
    case parseComplete
    case portalSuspended
    case readyForQuery(TransactionState)
    case rowDescription(RowDescription)
    case sslSupported
    case sslUnsupported
}
    
extension PSQLBackendMessage {
    enum ID: RawRepresentable, Equatable {
        typealias RawValue = UInt8
        
        case authentication
        case backendKeyData
        case bindComplete
        case closeComplete
        case commandComplete
        case copyData
        case copyDone
        case copyInResponse
        case copyOutResponse
        case copyBothResponse
        case dataRow
        case emptyQueryResponse
        case error
        case functionCallResponse
        case negotiateProtocolVersion
        case noData
        case noticeResponse
        case notificationResponse
        case parameterDescription
        case parameterStatus
        case parseComplete
        case portalSuspended
        case readyForQuery
        case rowDescription
        
        init?(rawValue: UInt8) {
            switch rawValue {
            case UInt8(ascii: "R"):
                self = .authentication
            case UInt8(ascii: "K"):
                self = .backendKeyData
            case UInt8(ascii: "2"):
                self = .bindComplete
            case UInt8(ascii: "3"):
                self = .closeComplete
            case UInt8(ascii: "C"):
                self = .commandComplete
            case UInt8(ascii: "d"):
                self = .copyData
            case UInt8(ascii: "c"):
                self = .copyDone
            case UInt8(ascii: "G"):
                self = .copyInResponse
            case UInt8(ascii: "H"):
                self = .copyOutResponse
            case UInt8(ascii: "W"):
                self = .copyBothResponse
            case UInt8(ascii: "D"):
                self = .dataRow
            case UInt8(ascii: "I"):
                self = .emptyQueryResponse
            case UInt8(ascii: "E"):
                self = .error
            case UInt8(ascii: "V"):
                self = .functionCallResponse
            case UInt8(ascii: "v"):
                self = .negotiateProtocolVersion
            case UInt8(ascii: "n"):
                self = .noData
            case UInt8(ascii: "N"):
                self = .noticeResponse
            case UInt8(ascii: "A"):
                self = .notificationResponse
            case UInt8(ascii: "t"):
                self = .parameterDescription
            case UInt8(ascii: "S"):
                self = .parameterStatus
            case UInt8(ascii: "1"):
                self = .parseComplete
            case UInt8(ascii: "s"):
                self = .portalSuspended
            case UInt8(ascii: "Z"):
                self = .readyForQuery
            case UInt8(ascii: "T"):
                self = .rowDescription
            default:
                return nil
            }
        }
        
        var rawValue: UInt8 {
            switch self {
            case .authentication:
                return UInt8(ascii: "R")
            case .backendKeyData:
                return UInt8(ascii: "K")
            case .bindComplete:
                return UInt8(ascii: "2")
            case .closeComplete:
                return UInt8(ascii: "3")
            case .commandComplete:
                return UInt8(ascii: "C")
            case .copyData:
                return UInt8(ascii: "d")
            case .copyDone:
                return UInt8(ascii: "c")
            case .copyInResponse:
                return UInt8(ascii: "G")
            case .copyOutResponse:
                return UInt8(ascii: "H")
            case .copyBothResponse:
                return UInt8(ascii: "W")
            case .dataRow:
                return UInt8(ascii: "D")
            case .emptyQueryResponse:
                return UInt8(ascii: "I")
            case .error:
                return UInt8(ascii: "E")
            case .functionCallResponse:
                return UInt8(ascii: "V")
            case .negotiateProtocolVersion:
                return UInt8(ascii: "v")
            case .noData:
                return UInt8(ascii: "n")
            case .noticeResponse:
                return UInt8(ascii: "N")
            case .notificationResponse:
                return UInt8(ascii: "A")
            case .parameterDescription:
                return UInt8(ascii: "t")
            case .parameterStatus:
                return UInt8(ascii: "S")
            case .parseComplete:
                return UInt8(ascii: "1")
            case .portalSuspended:
                return UInt8(ascii: "s")
            case .readyForQuery:
                return UInt8(ascii: "Z")
            case .rowDescription:
                return UInt8(ascii: "T")
            }
        }
    }
}

extension PSQLBackendMessage {
    
    static func decode(from buffer: inout ByteBuffer, for messageID: ID) throws -> PSQLBackendMessage {
        switch messageID {
        case .authentication:
            return try .authentication(.decode(from: &buffer))
            
        case .backendKeyData:
            return try .backendKeyData(.decode(from: &buffer))
            
        case .bindComplete:
            return .bindComplete
            
        case .closeComplete:
            return .closeComplete
            
        case .commandComplete:
            guard let commandTag = buffer.readNullTerminatedString() else {
                throw PSQLPartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .commandComplete(commandTag)
            
        case .dataRow:
            return try .dataRow(.decode(from: &buffer))
            
        case .emptyQueryResponse:
            return .emptyQueryResponse
            
        case .parameterStatus:
            return try .parameterStatus(.decode(from: &buffer))
            
        case .error:
            return try .error(.decode(from: &buffer))
            
        case .noData:
            return .noData
            
        case .noticeResponse:
            return try .notice(.decode(from: &buffer))
            
        case .notificationResponse:
            return try .notification(.decode(from: &buffer))
            
        case .parameterDescription:
            return try .parameterDescription(.decode(from: &buffer))
            
        case .parseComplete:
            return .parseComplete
            
        case .portalSuspended:
            return .portalSuspended
            
        case .readyForQuery:
            return try .readyForQuery(.decode(from: &buffer))
            
        case .rowDescription:
            return try .rowDescription(.decode(from: &buffer))
            
        case .copyData, .copyDone, .copyInResponse, .copyOutResponse, .copyBothResponse, .functionCallResponse, .negotiateProtocolVersion:
            preconditionFailure()
        }
    }
}

extension PSQLBackendMessage: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .authentication(let authentication):
            return ".authentication(\(String(reflecting: authentication)))"
        case .backendKeyData(let backendKeyData):
            return ".backendKeyData(\(String(reflecting: backendKeyData)))"
        case .bindComplete:
            return ".bindComplete"
        case .closeComplete:
            return ".closeComplete"
        case .commandComplete(let commandTag):
            return ".commandComplete(\(String(reflecting: commandTag)))"
        case .dataRow(let dataRow):
            return ".dataRow(\(String(reflecting: dataRow)))"
        case .emptyQueryResponse:
            return ".emptyQueryResponse"
        case .error(let error):
            return ".error(\(String(reflecting: error)))"
        case .noData:
            return ".noData"
        case .notice(let notice):
            return ".notice(\(String(reflecting: notice)))"
        case .notification(let notification):
            return ".notification(\(String(reflecting: notification)))"
        case .parameterDescription(let parameterDescription):
            return ".parameterDescription(\(String(reflecting: parameterDescription)))"
        case .parameterStatus(let parameterStatus):
            return ".parameterStatus(\(String(reflecting: parameterStatus)))"
        case .parseComplete:
            return ".parseComplete"
        case .portalSuspended:
            return ".portalSuspended"
        case .readyForQuery(let transactionState):
            return ".readyForQuery(\(String(reflecting: transactionState)))"
        case .rowDescription(let rowDescription):
            return ".rowDescription(\(String(reflecting: rowDescription)))"
        case .sslSupported:
            return ".sslSupported"
        case .sslUnsupported:
            return ".sslUnsupported"
        }
    }
}
