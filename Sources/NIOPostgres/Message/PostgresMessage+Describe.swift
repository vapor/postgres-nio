import NIO

extension PostgresMessage {
    /// Identifies the message as a Describe command.
    struct Describe: CustomStringConvertible, ByteBufferSerializable {
        /// Command type.
        enum Command: UInt8 {
            case statement = 0x53 // S
            case portal = 0x50 // P
        }
        
        /// 'S' to describe a prepared statement; or 'P' to describe a portal.
        let command: Command
        
        /// The name of the prepared statement or portal to describe
        /// (an empty string selects the unnamed prepared statement or portal).
        var name: String
        
        /// See `CustomStringConvertible`.
        var description: String {
            switch command {
            case .statement: return "Statement(" + name + ")"
            case .portal: return "Portal(" + name + ")"
            }
        }
        
        /// Serializes this message into a byte buffer.
        func serialize(into buffer: inout ByteBuffer) {
            buffer.write(integer: command.rawValue)
            buffer.write(nullTerminated: name)
        }
    }
}
