import NIO

extension PostgresMessage {
    /// Identifies the message as an Execute command.
    public struct Execute: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .execute
        }
        
        public var description: String {
            return "Execute()"
        }
        
        /// The name of the destination portal (an empty string selects the unnamed portal).
        public var portalName: String
        
        /// Maximum number of rows to return, if portal contains a query that
        /// returns rows (ignored otherwise). Zero denotes “no limit”.
        public var maxRows: Int32
        
        /// Serializes this message into a byte buffer.
        public func serialize(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(portalName)
            buffer.writeInteger(self.maxRows)
        }
    }
}
