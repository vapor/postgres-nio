import NIO

extension PostgresMessage {
    /// Identifies the message as cancellation key data.
    /// The frontend must save these values if it wishes to be able to issue CancelRequest messages later.
    struct BackendKeyData: CustomStringConvertible {
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> BackendKeyData {
            guard let processID = buffer.readInteger(as: Int32.self) else {
                throw PostgresError(.protocol("Could not parse process id from backend key data"))
            }
            guard let secretKey = buffer.readInteger(as: Int32.self) else {
                throw PostgresError(.protocol("Could not parse secret key from backend key data"))
            }
            return .init(processID: processID, secretKey: secretKey)
        }
        
        /// The process ID of this backend.
        var processID: Int32
        
        /// The secret key of this backend.
        var secretKey: Int32
        
        /// See `CustomStringConvertible`.
        var description: String {
            return "processID: \(processID), secretKey: \(secretKey)"
        }
    }
}
