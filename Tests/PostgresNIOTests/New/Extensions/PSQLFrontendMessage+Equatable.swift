import NIOCore
@testable import PostgresNIO
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

extension PSQLFrontendMessage: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.bind(let lhs), .bind(let rhs)):
            return lhs == rhs
        case (.cancel(let lhs), .cancel(let rhs)):
            return lhs == rhs
        case (.close(let lhs), .close(let rhs)):
            return lhs == rhs
        case (.describe(let lhs), .describe(let rhs)):
            return lhs == rhs
        case (.execute(let lhs), .execute(let rhs)):
            return lhs == rhs
        case (.flush, .flush):
            return true
        case (.parse(let lhs), .parse(let rhs)):
            return lhs == rhs
        case (.password(let lhs), .password(let rhs)):
            return lhs == rhs
        case (.saslInitialResponse(let lhs), .saslInitialResponse(let rhs)):
            return lhs == rhs
        case (.saslResponse(let lhs), .saslResponse(let rhs)):
            return lhs == rhs
        case (.sslRequest(let lhs), .sslRequest(let rhs)):
            return lhs == rhs
        case (.sync, .sync):
            return true
        case (.startup(let lhs), .startup(let rhs)):
            return lhs == rhs
        case (.terminate, .terminate):
            return lhs == rhs
        default:
            return false
        }
    }
}
