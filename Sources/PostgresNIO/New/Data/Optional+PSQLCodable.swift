import NIOCore

extension Optional: PostgresDecodable where Wrapped: PostgresDecodable, Wrapped.DecodableType == Wrapped {
    typealias DecodableType = Wrapped

    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Optional<Wrapped> {
        preconditionFailure("This should not be called")
    }

    static func decodeRaw<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard var buffer = byteBuffer else {
            return nil
        }
        return try DecodableType.decode(from: &buffer, type: type, format: format, context: context)
    }
}

extension Optional: PSQLEncodable where Wrapped: PSQLEncodable {
    var psqlType: PostgresDataType {
        switch self {
        case .some(let value):
            return value.psqlType
        case .none:
            return .null
        }
    }
    
    var psqlFormat: PostgresFormat {
        switch self {
        case .some(let value):
            return value.psqlFormat
        case .none:
            return .binary
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        preconditionFailure("Should never be hit, since `encodeRaw` is implemented.")
    }
    
    func encodeRaw(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        switch self {
        case .none:
            byteBuffer.writeInteger(-1, as: Int32.self)
        case .some(let value):
            try value.encodeRaw(into: &byteBuffer, context: context)
        }
    }
}

extension Optional: PSQLCodable where Wrapped: PSQLCodable, Wrapped.DecodableType == Wrapped {
    
}
