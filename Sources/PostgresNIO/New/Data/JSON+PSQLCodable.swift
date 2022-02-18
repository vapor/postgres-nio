import NIOCore
import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

private let JSONBVersionByte: UInt8 = 0x01

extension PSQLEncodable where Self: Encodable {
    public var psqlType: PostgresDataType {
        .jsonb
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        buffer.writeInteger(JSONBVersionByte)
        try context.jsonEncoder.encode(self, into: &buffer)
    }
}

extension PSQLDecodable where Self: Decodable {
    static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        switch (format, type) {
        case (.binary, .jsonb):
            guard JSONBVersionByte == buffer.readInteger(as: UInt8.self) else {
                throw PostgresCastingError.Code.failure
            }
            return try context.jsonDecoder.decode(Self.self, from: buffer)
        case (.binary, .json), (.text, .jsonb), (.text, .json):
            return try context.jsonDecoder.decode(Self.self, from: buffer)
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

