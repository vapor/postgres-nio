import NIOCore

extension PostgresMessage {
    /// Identifies the message as a Describe command.
    @available(*, deprecated, message: "Will be removed from public API")
    public struct Describe {
        public static var identifier: PostgresMessage.Identifier {
            return .describe
        }
        
        /// Command type.
        public enum Command: UInt8 {
            case statement = 0x53 // S
            case portal = 0x50 // P
        }
        
        /// 'S' to describe a prepared statement; or 'P' to describe a portal.
        public let command: Command
        
        /// The name of the prepared statement or portal to describe
        /// (an empty string selects the unnamed prepared statement or portal).
        public var name: String
        
        /// See `CustomStringConvertible`.
        public var description: String {
            switch command {
            case .statement: return "Statement(" + name + ")"
            case .portal: return "Portal(" + name + ")"
            }
        }
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeInteger(command.rawValue)
            buffer.writeNullTerminatedString(name)
        }
    }
}
