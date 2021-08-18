import NIOCore

/// A frontend or backend Postgres message.
public struct PostgresMessage: Equatable {
    public var identifier: Identifier
    public var data: ByteBuffer

    public init<Data>(identifier: Identifier, bytes: Data)
        where Data: Sequence, Data.Element == UInt8
    {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeBytes(bytes)
        self.init(identifier: identifier, data: buffer)
    }

    public init(identifier: Identifier, data: ByteBuffer) {
        self.identifier = identifier
        self.data = data
    }
}
