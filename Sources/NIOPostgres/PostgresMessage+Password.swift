import NIO

extension PostgresMessage {
    /// Identifies the message as a password response. Note that this is also used for
    /// GSSAPI and SSPI response messages (which is really a design error, since the contained
    /// data is not a null-terminated string in that case, but can be arbitrary binary data).
    struct Password {
        /// The password (encrypted, if requested).
        var string: String
        
        /// Serializes this message into a byte buffer.
        func serialize(into buffer: inout ByteBuffer) {
            buffer.write(string: string + "\0")
        }
    }
}
