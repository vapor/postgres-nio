import NIOCore

extension PostgresMessage {
    /// Identifies the message as a Bind command.
    @available(*, deprecated, message: "Will be removed from public API")
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
