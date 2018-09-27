import NIO

extension PostgresMessage {
    /// Identifies the message as a Close command.
    struct CommandComplete {
        /// Parses an instance of this message type from a byte buffer.
        static func parse(from buffer: inout ByteBuffer) throws -> CommandComplete {
            guard let string = buffer.readNullTerminatedString() else {
                throw PostgresError(.protocol("Could not parse close response message"))
            }
            return .init(tag: string)
        }
        
        /// The command tag. This is usually a single word that identifies which SQL command was completed.
        var tag: String
    }
}
