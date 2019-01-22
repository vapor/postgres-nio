/// A frontend or backend Postgres message.
public struct PostgresMessage {
    public var identifier: Identifier
    
    public var data: ByteBuffer
    
    public init(identifier: Identifier, data: ByteBuffer) {
        self.identifier = identifier
        self.data = data
    }
}
