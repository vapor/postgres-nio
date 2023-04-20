import NIOCore

/// A frontend or backend Postgres message.
public struct PostgresMessage: Equatable {
    @available(*, deprecated, message: "Will be removed from public API.")
    public var identifier: Identifier
    public var data: ByteBuffer

    @available(*, deprecated, message: "Will be removed from public API.")
    public init<Data>(identifier: Identifier, bytes: Data)
        where Data: Sequence, Data.Element == UInt8
    {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeBytes(bytes)
        self.init(identifier: identifier, data: buffer)
    }

    @available(*, deprecated, message: "Will be removed from public API.")
    public init(identifier: Identifier, data: ByteBuffer) {
        self.identifier = identifier
        self.data = data
    }
}
