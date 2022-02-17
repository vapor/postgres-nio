import NIOCore

struct PSQLData: Equatable {
    
    @usableFromInline var bytes: ByteBuffer?
    @usableFromInline var dataType: PostgresDataType
    @usableFromInline var format: PostgresFormat
    
    /// use this only for testing
    init(bytes: ByteBuffer?, dataType: PostgresDataType, format: PostgresFormat) {
        self.bytes = bytes
        self.dataType = dataType
        self.format = format
    }
}
