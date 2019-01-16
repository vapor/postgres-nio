public protocol PostgresMessageType: CustomStringConvertible {
    static var identifier: PostgresMessage.Identifier { get }
    
    #warning("TODO: rename parse/serialize to parseData/serializeData")
    static func parse(from buffer: inout ByteBuffer) throws -> Self
    func serialize(into buffer: inout ByteBuffer) throws
}

extension PostgresMessageType {
    public static var identifier: PostgresMessage.Identifier {
        return .none
    }
    
    public static func parse(from buffer: inout ByteBuffer) throws -> PostgresMessage.SSLRequest {
        fatalError("\(Self.self) does not support parsing.")
    }
    
    public func serialize(into buffer: inout ByteBuffer) throws {
        fatalError("\(Self.self) does not support serializing.")
    }
}

extension PostgresMessageType {
    public static func parse(from message: inout PostgresMessage) throws -> Self {
        guard message.identifier == Self.identifier else {
            fatalError("Message identifier does not match.")
        }
        return try Self.parse(from: &message.data)
    }
    
    public func serialize(to message: inout PostgresMessage) throws {
        try self.serialize(into: &message.data)
        message.identifier = Self.identifier
    }
}
