import NIOCore

extension PostgresMessage {
    /// A message asking the PostgreSQL server if SSL is supported
    /// For more info, see https://www.postgresql.org/docs/10/static/protocol-flow.html#id-1.10.5.7.11
    @available(*, deprecated, message: "Will be removed from public API")
    public struct SSLRequest {
        /// The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits,
        /// and 5679 in the least significant 16 bits.
        public let code: Int32
        
        /// See `CustomStringConvertible`.
        public var description: String {
            return "SSLRequest"
        }
        
        /// Creates a new `SSLRequest`.
        public init() {
            self.code = 80877103
        }
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeInteger(self.code)
        }
    }
}
