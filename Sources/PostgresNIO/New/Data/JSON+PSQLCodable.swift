import NIOCore
import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

private let JSONBVersionByte: UInt8 = 0x01

extension PSQLCodable where Self: Codable {
    var psqlType: PostgresDataType {
        .jsonb
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    static func decode<JSONDecoder: PostgresJSONDecoder>(
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
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        byteBuffer.writeInteger(JSONBVersionByte)
        try context.jsonEncoder.encode(self, into: &byteBuffer)
    }
}
