import NIOCore

extension Optional: PostgresDecodable where Wrapped: PostgresDecodable, Wrapped.DecodableType == Wrapped {
    public typealias DecodableType = Wrapped

    public static func decode<JSONDecoder : PostgresJSONDecoder>(from byteBuffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PostgresDecodingContext<JSONDecoder>) throws -> Optional<Wrapped> {
        preconditionFailure("This should not be called")
    }

    @inlinable
    public static func decodeRaw<JSONDecoder : PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Optional<Wrapped> {
        switch byteBuffer {
        case .some(var buffer):
            return try Wrapped.decode(from: &buffer, type: type, format: format, context: context)
        case .none:
            return .none
        }
    }
}

extension Optional: PSQLEncodable where Wrapped: PSQLEncodable {
    public var psqlType: PostgresDataType {
        switch self {
        case .some(let value):
            return value.psqlType
        case .none:
            return .null
        }
    }
    
    public var psqlFormat: PostgresFormat {
        switch self {
        case .some(let value):
            return value.psqlFormat
        case .none:
            return .binary
        }
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) {
        preconditionFailure("Should never be hit, since `encodeRaw` is implemented.")
    }
    
    public func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        switch self {
        case .none:
            buffer.writeInteger(-1, as: Int32.self)
        case .some(let value):
            try value.encodeRaw(into: &buffer, context: context)
        }
    }
}

extension Optional: PSQLCodable where Wrapped: PSQLCodable, Wrapped.DecodableType == Wrapped {

}
