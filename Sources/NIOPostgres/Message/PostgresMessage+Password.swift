import NIO

extension PostgresMessage {
    /// Identifies the message as a password response. Note that this is also used for
    /// GSSAPI and SSPI response messages (which is really a design error, since the contained
    /// data is not a null-terminated string in that case, but can be arbitrary binary data).
    public struct Password: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .passwordMessage
        }
        
        public static func parse(from buffer: inout ByteBuffer) throws -> PostgresMessage.Password {
            fatalError()
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
            buffer.write(string: string + "\0")
        }
    }
}
