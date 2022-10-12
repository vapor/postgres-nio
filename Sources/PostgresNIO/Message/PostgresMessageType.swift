import NIOCore

@available(*, deprecated, message: "Will be removed from public API. Internally we now use `PostgresBackendMessage` and `PostgresFrontendMessage`")
public protocol PostgresMessageType {
    static var identifier: PostgresMessage.Identifier { get }
    static func parse(from buffer: inout ByteBuffer) throws -> Self
    func serialize(into buffer: inout ByteBuffer) throws
}

@available(*, deprecated, message: "`PostgresMessageType` protocol is deprecated.")
extension PostgresMessageType {
    @available(*, deprecated, message: "Will be removed from public API.")
    func message() throws -> PostgresMessage {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        try self.serialize(into: &buffer)
        return .init(identifier: Self.identifier, data: buffer)
    }
    
    public init(message: PostgresMessage) throws {
        var message = message
        self = try Self.parse(from: &message.data)
    }

    @available(*, deprecated, message: "Will be removed from public API.")
    public static var identifier: PostgresMessage.Identifier {
        return .none
    }
    
    public static func parse(from buffer: inout ByteBuffer) throws -> Self {
        fatalError("\(Self.self) does not support parsing.")
    }
    
    public func serialize(into buffer: inout ByteBuffer) throws {
        fatalError("\(Self.self) does not support serializing.")
    }
}
