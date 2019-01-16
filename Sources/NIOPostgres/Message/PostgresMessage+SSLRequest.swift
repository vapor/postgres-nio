import NIO

extension PostgresMessage {
    /// A message asking the PostgreSQL server if SSL is supported
    /// For more info, see https://www.postgresql.org/docs/10/static/protocol-flow.html#id-1.10.5.7.11
    public struct SSLRequest: PostgresMessageType {
        public static func parse(from buffer: inout ByteBuffer) throws -> PostgresMessage.SSLRequest {
            fatalError()
        }
        
        public static var identifier: PostgresMessage.Identifier {
            return .none
        }
        
        public var description: String {
            return "SSLRequest()"
        }
        
        /// The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits,
        /// and 5679 in the least significant 16 bits.
        public let code: Int32
        
        public init() {
            self.code = 80877103
        }
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.write(integer: self.code)
        }
    }
}
