import NIOCore

extension PSQLFrontendMessage {
    /// A message asking the PostgreSQL server if TLS is supported
    /// For more info, see https://www.postgresql.org/docs/10/static/protocol-flow.html#id-1.10.5.7.11
    struct SSLRequest: PayloadEncodable, Equatable {
        /// The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits,
        /// and 5679 in the least significant 16 bits.
        let code: Int32
        
        /// Creates a new `SSLRequest`.
        init() {
            self.code = 80877103
        }
        
        /// Serializes this message into a byte buffer.
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeInteger(self.code)
        }
    }
}
