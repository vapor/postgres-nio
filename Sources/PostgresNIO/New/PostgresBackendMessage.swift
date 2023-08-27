import NIOCore
//import struct Foundation.Data


/// A protocol to implement for all associated value in the `PostgresBackendMessage` enum
protocol PSQLMessagePayloadDecodable {
    
    /// Decodes the associated value for a `PostgresBackendMessage` from the given `ByteBuffer`.
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
enum PostgresBackendMessage: Hashable {
    
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
    
extension PostgresBackendMessage {
    enum ID: UInt8, Hashable {
        case authentication = 82            // ascii: R
        case backendKeyData = 75            // ascii: K
        case bindComplete = 50              // ascii: 2
        case closeComplete = 51             // ascii: 3
        case commandComplete = 67           // ascii: C
        case copyData = 100                 // ascii: d
        case copyDone = 99                  // ascii: c
        case copyInResponse = 71            // ascii: G
        case copyOutResponse = 72           // ascii: H
        case copyBothResponse = 87          // ascii: W
        case dataRow = 68                   // ascii: D
        case emptyQueryResponse = 73        // ascii: I
        case error = 69                     // ascii: E
        case functionCallResponse = 86      // ascii: V
        case negotiateProtocolVersion = 118 // ascii: v
        case noData = 110                   // ascii: n
        case noticeResponse = 78            // ascii: N
        case notificationResponse = 65      // ascii: A
        case parameterDescription = 116     // ascii: t
        case parameterStatus = 83           // ascii: S
        case parseComplete = 49             // ascii: 1
        case portalSuspended = 115          // ascii: s
        case readyForQuery = 90             // ascii: Z
        case rowDescription = 84            // ascii: T
    }
}

extension PostgresBackendMessage {
    
    static func decode(from buffer: inout ByteBuffer, for messageID: ID) throws -> PostgresBackendMessage {
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

extension PostgresBackendMessage: CustomDebugStringConvertible {
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
