import NIOCore

extension PostgresMessage {
    /// Identifies the message as a simple query.
    public struct SimpleQuery: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .query
        }
        
        /// The query string itself.
        public var string: String
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeString(self.string + "\0")
        }
    }
}
