import NIOCore

extension Optional: PSQLDecodable where Wrapped: PSQLDecodable {
    typealias ActualType = Wrapped

    static func decode(from byteBuffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Optional<Wrapped> {
        preconditionFailure("This should not be called")
    }

    static func decodeRaw(from byteBuffer: inout ByteBuffer?, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch byteBuffer {
        case .some(var buffer):
            return try ActualType.decode(from: &buffer, type: type, format: format, context: context)
        case .none:
            return nil
        }
    }
}

extension Optional: PSQLEncodable where Wrapped: PSQLEncodable {
    var psqlType: PSQLDataType {
        switch self {
        case .some(let value):
            return value.psqlType
        case .none:
            return .null
        }
    }
    
    var psqlFormat: PSQLFormat {
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

extension Optional: PSQLCodable where Wrapped: PSQLCodable {
    
}
