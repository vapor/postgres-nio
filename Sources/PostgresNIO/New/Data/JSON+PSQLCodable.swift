import NIOCore
import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

private let JSONBVersionByte: UInt8 = 0x01

extension PSQLCodable where Self: Codable {
    var psqlType: PSQLDataType {
        .jsonb
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        switch (format, type) {
        case (.binary, .jsonb):
            guard JSONBVersionByte == buffer.readInteger(as: UInt8.self) else {
                throw PSQLCastingError.Code.failure
            }
            return try context.jsonDecoder.decode(Self.self, from: buffer)
        case (.binary, .json), (.text, .jsonb), (.text, .json):
            return try context.jsonDecoder.decode(Self.self, from: buffer)
        default:
            throw PSQLCastingError.Code.typeMismatch
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        byteBuffer.writeInteger(JSONBVersionByte)
        try context.jsonEncoder.encode(self, into: &byteBuffer)
    }
}
