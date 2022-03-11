import NIOCore
import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

@usableFromInline
let JSONBVersionByte: UInt8 = 0x01

extension PostgresEncodable where Self: Encodable {
    public static var psqlType: PostgresDataType {
        .jsonb
    }
    
    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        byteBuffer.writeInteger(JSONBVersionByte)
        try context.jsonEncoder.encode(self, into: &byteBuffer)
    }
}

extension PostgresDecodable where Self: Decodable {
    init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .jsonb):
            guard JSONBVersionByte == buffer.readInteger(as: UInt8.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = try context.jsonDecoder.decode(Self.self, from: buffer)
        case (.binary, .json), (.text, .jsonb), (.text, .json):
            self = try context.jsonDecoder.decode(Self.self, from: buffer)
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension PostgresCodable where Self: Codable {}
