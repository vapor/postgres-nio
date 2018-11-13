import NIO

extension PostgresMessage {
    /// Response given after sending a `PostgreSQLSSLSupportRequest`.
    /// See https://www.postgresql.org/docs/10/static/protocol-flow.html#id-1.10.5.7.11 for more info.
    enum SSLResponse: UInt8 {
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> SSLResponse {
            guard let status = buffer.readInteger(rawRepresentable: SSLResponse.self) else {
                throw PostgresError(.protocol("Could not read SSL support response"))
            }
            return status
        }
        
        /// The server supports SSL (char S).
        case supported = 0x53
        /// The server does not support SSL (char N).
        case unsupported = 0x4E
    }
}
