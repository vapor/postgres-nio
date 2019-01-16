extension PostgresMessage {
    public struct Sync: PostgresMessageType {
        public static func parse(from buffer: inout ByteBuffer) throws -> PostgresMessage.Sync {
            return .init()
        }
        
        public static var identifier: PostgresMessage.Identifier {
            return .sync
        }
        
        public var description: String {
            return "Sync()"
        }
        
        public init() {}
        
        
        public func serialize(into buffer: inout ByteBuffer) throws {
            
        }
        
    }
}
