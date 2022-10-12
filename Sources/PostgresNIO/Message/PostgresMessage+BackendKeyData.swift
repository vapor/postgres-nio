import NIOCore

extension PostgresMessage {
    /// Identifies the message as cancellation key data.
    /// The frontend must save these values if it wishes to be able to issue CancelRequest messages later.
    public struct BackendKeyData {
        /// The process ID of this backend.
        public var processID: Int32
        
        /// The secret key of this backend.
        public var secretKey: Int32
    }
}

@available(*, deprecated, message: "Deprecating conformance to `PostgresMessageType` since it is deprecated.")
extension PostgresMessage.BackendKeyData: PostgresMessageType {
    public static var identifier: PostgresMessage.Identifier {
        .backendKeyData
    }

    /// Parses an instance of this message type from a byte buffer.
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        guard let processID = buffer.readInteger(as: Int32.self) else {
            throw PostgresError.protocol("Could not parse process id from backend key data")
        }
        guard let secretKey = buffer.readInteger(as: Int32.self) else {
            throw PostgresError.protocol("Could not parse secret key from backend key data")
        }
        return .init(processID: processID, secretKey: secretKey)
    }
}
