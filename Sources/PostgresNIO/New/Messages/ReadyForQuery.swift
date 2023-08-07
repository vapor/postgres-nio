import NIOCore

extension PostgresBackendMessage {
    enum TransactionState: UInt8, PayloadDecodable, Hashable {
        case idle = 73 // ascii: I
        case inTransaction = 84 // ascii: T
        case inFailedTransaction = 69 // ascii: E

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let value = try buffer.throwingReadInteger(as: UInt8.self)
            guard let state = Self.init(rawValue: value) else {
                throw PSQLPartialDecodingError.valueNotRawRepresentable(value: value, asType: TransactionState.self)
            }
            
            return state
        }
    }
}

extension PostgresBackendMessage.TransactionState: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .idle:
            return ".idle"
        case .inTransaction:
            return ".inTransaction"
        case .inFailedTransaction:
            return ".inFailedTransaction"
        }
    }
}
