import NIOCore

extension Optional: PSQLDecodable where Wrapped: PSQLDecodable {
    @usableFromInline
    typealias ActualType = Wrapped

    public static func decode<JSONDecoder : PSQLJSONDecoder>(from byteBuffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext<JSONDecoder>) throws -> Optional<Wrapped> {
        preconditionFailure("This should not be called")
    }

    @inlinable
    public static func decodeRaw<JSONDecoder : PSQLJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        switch byteBuffer {
        case .some(var buffer):
            return try ActualType.decode(from: &buffer, type: type, format: format, context: context)
        case .none:
            return nil
        }
    }
}

extension Optional: PSQLEncodable where Wrapped: PSQLEncodable {
    public var psqlType: PSQLDataType {
        switch self {
        case .some(let value):
            return value.psqlType
        case .none:
            return .null
        }
    }
    
    public var psqlFormat: PSQLFormat {
        switch self {
        case .some(let value):
            return value.psqlFormat
        case .none:
            return .binary
        }
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        preconditionFailure("Should never be hit, since `encodeRaw` is implemented.")
    }
    
    public func encodeRaw<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        switch self {
        case .none:
            buffer.writeInteger(-1, as: Int32.self)
        case .some(let value):
            try value.encodeRaw(into: &buffer, context: context)
        }
    }
}

extension Optional: PSQLCodable where Wrapped: PSQLCodable {
    
}
