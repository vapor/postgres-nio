import NIOCore

/// A wire message that is created by a Postgres client to be consumed by Postgres server.
///
/// All messages are defined in the official Postgres Documentation in the section
/// [Frontend/Backend Protocol – Message Formats](https://www.postgresql.org/docs/13/protocol-message-formats.html)
enum PostgresFrontendMessage: Equatable {
    case bind(Bind)
    case cancel(Cancel)
    case close(Close)
    case describe(Describe)
    case execute(Execute)
    case flush
    case parse(Parse)
    case password(Password)
    case saslInitialResponse(SASLInitialResponse)
    case saslResponse(SASLResponse)
    case sslRequest(SSLRequest)
    case sync
    case startup(Startup)
    case terminate
    
    enum ID: UInt8, Equatable {
        
        case bind
        case close
        case describe
        case execute
        case flush
        case parse
        case password
        case saslInitialResponse
        case saslResponse
        case sync
        case terminate
        
        init?(rawValue: UInt8) {
            switch rawValue {
            case UInt8(ascii: "B"):
                self = .bind
            case UInt8(ascii: "C"):
                self = .close
            case UInt8(ascii: "D"):
                self = .describe
            case UInt8(ascii: "E"):
                self = .execute
            case UInt8(ascii: "H"):
                self = .flush
            case UInt8(ascii: "P"):
                self = .parse
            case UInt8(ascii: "p"):
                self = .password
            case UInt8(ascii: "p"):
                self = .saslInitialResponse
            case UInt8(ascii: "p"):
                self = .saslResponse
            case UInt8(ascii: "S"):
                self = .sync
            case UInt8(ascii: "X"):
                self = .terminate
            default:
                return nil
            }
        }

        var rawValue: UInt8 {
            switch self {
            case .bind:
                return UInt8(ascii: "B")
            case .close:
                return UInt8(ascii: "C")
            case .describe:
                return UInt8(ascii: "D")
            case .execute:
                return UInt8(ascii: "E")
            case .flush:
                return UInt8(ascii: "H")
            case .parse:
                return UInt8(ascii: "P")
            case .password:
                return UInt8(ascii: "p")
            case .saslInitialResponse:
                return UInt8(ascii: "p")
            case .saslResponse:
                return UInt8(ascii: "p")
            case .sync:
                return UInt8(ascii: "S")
            case .terminate:
                return UInt8(ascii: "X")
            }
        }
    }
}

extension PostgresFrontendMessage {
    
    var id: ID {
        switch self {
        case .bind:
            return .bind
        case .cancel:
            preconditionFailure("Cancel messages don't have an identifier")
        case .close:
            return .close
        case .describe:
            return .describe
        case .execute:
            return .execute
        case .flush:
            return .flush
        case .parse:
            return .parse
        case .password:
            return .password
        case .saslInitialResponse:
            return .saslInitialResponse
        case .saslResponse:
            return .saslResponse
        case .sslRequest:
            preconditionFailure("SSL requests don't have an identifier")
        case .startup:
            preconditionFailure("Startup messages don't have an identifier")
        case .sync:
            return .sync
        case .terminate:
            return .terminate

        }
    }
}

protocol PSQLMessagePayloadEncodable {
    func encode(into buffer: inout ByteBuffer)
}
