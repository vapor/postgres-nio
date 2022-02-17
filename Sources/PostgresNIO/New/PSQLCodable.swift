import NIOCore

/// A type that can encode itself to a postgres wire binary representation.
protocol PSQLEncodable {
    /// identifies the data type that we will encode into `byteBuffer` in `encode`
    var psqlType: PostgresDataType { get }
    
    /// identifies the postgres format that is used to encode the value into `byteBuffer` in `encode`
    var psqlFormat: PostgresFormat { get }
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format, without setting
    /// the byte count. This method is called from the default `encodeRaw` implementation.
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format including its
    /// leading byte count. This method has a default implementation and may be overriden
    /// only for special cases, like `Optional`s.
    func encodeRaw(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws
}

/// A type that can decode itself from a postgres wire binary representation.
protocol PSQLDecodable {
    associatedtype DecodableType: PSQLDecodable = Self

    /// Decode an entity from the `byteBuffer` in postgres wire format
    ///
    /// - Parameters:
    ///   - byteBuffer: A `ByteBuffer` to decode. The byteBuffer is sliced in such a way that it is expected
    ///                 that the complete buffer is consumed for decoding
    ///   - type: The postgres data type. Depending on this type the `byteBuffer`'s bytes need to be interpreted
    ///           in different ways.
    ///   - format: The postgres wire format. Can be `.text` or `.binary`
    ///   - context: A `PSQLDecodingContext` providing context for decoding. This includes a `JSONDecoder`
    ///              to use when decoding json and metadata to create better errors.
    /// - Returns: A decoded object
    static func decode(from byteBuffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PSQLDecodingContext) throws -> Self

    /// Decode an entity from the `byteBuffer` in postgres wire format.
    /// This method has a default implementation and may be overriden
    /// only for special cases, like `Optional`s.
    static func decodeRaw(from byteBuffer: inout ByteBuffer?, type: PostgresDataType, format: PostgresFormat, context: PSQLDecodingContext) throws -> Self
}

extension PSQLDecodable {
    @inlinable
    public static func decodeRaw(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PSQLDecodingContext
    ) throws -> Self {
        guard var buffer = byteBuffer else {
            throw PostgresCastingError.Code.missingData
        }
        return try self.decode(from: &buffer, type: type, format: format, context: context)
    }
}

/// A type that can be encoded into and decoded from a postgres binary format
protocol PSQLCodable: PSQLEncodable, PSQLDecodable {}

extension PSQLEncodable {
    func encodeRaw(into buffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        // The length of the parameter value, in bytes (this count does not include
        // itself). Can be zero.
        let lengthIndex = buffer.writerIndex
        buffer.writeInteger(0, as: Int32.self)
        let startIndex = buffer.writerIndex
        // The value of the parameter, in the format indicated by the associated format
        // code. n is the above length.
        try self.encode(into: &buffer, context: context)
        
        // overwrite the empty length, with the real value
        buffer.setInteger(numericCast(buffer.writerIndex - startIndex), at: lengthIndex, as: Int32.self)
    }
}

struct PSQLEncodingContext {
    let jsonEncoder: PostgresJSONEncoder
}

struct PSQLDecodingContext {
    
    let jsonDecoder: PostgresJSONDecoder
    
    let columnIndex: Int
    let columnName: String
    
    let file: String
    let line: Int
    
    init(jsonDecoder: PostgresJSONDecoder, columnName: String, columnIndex: Int, file: String, line: Int) {
        self.jsonDecoder = jsonDecoder
        self.columnName = columnName
        self.columnIndex = columnIndex
        
        self.file = file
        self.line = line
    }
}
