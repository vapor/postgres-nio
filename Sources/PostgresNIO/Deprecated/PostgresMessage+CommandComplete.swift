import NIOCore

extension PostgresMessage {
    /// Identifies the message as a Close command.
    @available(*, deprecated, message: "Will be removed from public API")
    public struct CommandComplete {
        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> CommandComplete {
            guard let string = buffer.readNullTerminatedString() else {
                throw PostgresError.protocol("Could not parse close response message")
            }
            return .init(tag: string)
        }
        
        /// The command tag. This is usually a single word that identifies which SQL command was completed.
        public var tag: String
    }
}
