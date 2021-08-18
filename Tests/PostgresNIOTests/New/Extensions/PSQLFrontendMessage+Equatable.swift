import NIOCore
@testable import PostgresNIO
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

extension PSQLFrontendMessage.Bind: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        guard lhs.preparedStatementName == rhs.preparedStatementName else {
            return false
        }
        
        guard lhs.portalName == rhs.portalName else {
            return false
        }
        
        guard lhs.parameters.count == rhs.parameters.count else {
            return false
        }
        
        var lhsIterator = lhs.parameters.makeIterator()
        var rhsIterator = rhs.parameters.makeIterator()
        
        do {
            while let lhs = lhsIterator.next(), let rhs = rhsIterator.next() {
                guard lhs.psqlType == rhs.psqlType else {
                    return false
                }
                
                var lhsBuffer = ByteBuffer()
                var rhsBuffer = ByteBuffer()
                
                try lhs.encode(into: &lhsBuffer, context: .forTests())
                try rhs.encode(into: &rhsBuffer, context: .forTests())
                
                guard lhsBuffer == rhsBuffer else {
                    return false
                }
            }
            
            return true
        } catch {
            return false
        }
    }
}

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
