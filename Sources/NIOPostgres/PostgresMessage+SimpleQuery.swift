import NIO

extension PostgresMessage {
    /// Identifies the message as a simple query.
    struct SimpleQuery {
        /// The query string itself.
        var string: String
        
        /// Serializes this message into a byte buffer.
        func serialize(into buffer: inout ByteBuffer) {
            buffer.write(string: string + "\0")
        }
    }
}
