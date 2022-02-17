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
    
    @inlinable
    func decode<T: PSQLDecodable>(as: Optional<T>.Type, context: PSQLDecodingContext) throws -> T? {
        try self.decodeIfPresent(as: T.self, context: context)
    }
    
    @inlinable
    func decode<T: PSQLDecodable>(as type: T.Type, context: PSQLDecodingContext) throws -> T {
        switch self.bytes {
        case .none:
            throw PSQLCastingError.missingData(targetType: type, type: self.dataType, context: context)
        case .some(var buffer):
            return try T.decode(from: &buffer, type: self.dataType, format: self.format, context: context)
        }
    }
    
    @inlinable
    func decodeIfPresent<T: PSQLDecodable>(as: T.Type, context: PSQLDecodingContext) throws -> T? {
        switch self.bytes {
        case .none:
            return nil
        case .some(var buffer):
            return try T.decode(from: &buffer, type: self.dataType, format: self.format, context: context)
        }
    }
}
