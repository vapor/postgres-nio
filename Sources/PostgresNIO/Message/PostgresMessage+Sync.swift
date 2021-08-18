import NIOCore

extension PostgresMessage {
    /// Identifies the message as a Bind command.
    public struct Sync: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            return .sync
        }
        
        public var description: String {
            return "Sync"
        }
        
        public func serialize(into buffer: inout ByteBuffer) {
            
        }
    }
}
