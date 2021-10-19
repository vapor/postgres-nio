@testable import PostgresNIO
import class Foundation.JSONEncoder

extension PSQLBackendMessage: Equatable {
    
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.authentication(let lhs), .authentication(let rhs)):
            return lhs == rhs
        case (.backendKeyData(let lhs), .backendKeyData(let rhs)):
            return lhs == rhs
        case (.bindComplete, bindComplete):
            return true
        case (.closeComplete, closeComplete):
            return true
        case (.commandComplete(let lhs), commandComplete(let rhs)):
            return lhs == rhs
        case (.dataRow(let lhs), dataRow(let rhs)):
            return lhs == rhs
        case (.emptyQueryResponse, emptyQueryResponse):
            return true
        case (.error(let lhs), error(let rhs)):
            return lhs == rhs
        case (.noData, noData):
            return true
        case (.notice(let lhs), notice(let rhs)):
            return lhs == rhs
        case (.notification(let lhs), .notification(let rhs)):
            return lhs == rhs
        case (.parameterDescription(let lhs), parameterDescription(let rhs)):
            return lhs == rhs
        case (.parameterStatus(let lhs), parameterStatus(let rhs)):
            return lhs == rhs
        case (.parseComplete, parseComplete):
            return true
        case (.portalSuspended, portalSuspended):
            return true
        case (.readyForQuery(let lhs), readyForQuery(let rhs)):
            return lhs == rhs
        case (.rowDescription(let lhs), rowDescription(let rhs)):
            return lhs == rhs
        case (.sslSupported, sslSupported):
            return true
        case (.sslUnsupported, sslUnsupported):
            return true
        default:
            return false
        }
    }
}

extension DataRow: ExpressibleByArrayLiteral {
    public typealias ArrayLiteralElement = PSQLEncodable

    public init(arrayLiteral elements: PSQLEncodable...) {
        
        var buffer = ByteBuffer()
        let encodingContext = PSQLEncodingContext(jsonEncoder: JSONEncoder())
        elements.forEach { element in
            try! element.encodeRaw(into: &buffer, context: encodingContext)
        }
        
        self.init(columnCount: Int16(elements.count), bytes: buffer)
    }
}
