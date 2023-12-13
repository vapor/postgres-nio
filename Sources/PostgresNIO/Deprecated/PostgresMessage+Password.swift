import NIOCore

extension PostgresMessage {
    /// Identifies the message as a password response. Note that this is also used for
    /// GSSAPI and SSPI response messages (which is really a design error, since the contained
    /// data is not a null-terminated string in that case, but can be arbitrary binary data).
    @available(*, deprecated, message: "Will be removed from public API")
    public struct Password {
        public static var identifier: PostgresMessage.Identifier {
            return .passwordMessage
        }
        
        public init(string: String) {
            self.string = string
        }
        
        /// The password (encrypted, if requested).
        public var string: String
        
        public var description: String {
            return "Password(\(string))"
        }
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeString(self.string + "\0")
        }
    }
}
