import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

private let JSONBVersionByte: UInt8 = 0x01

extension PSQLCodable where Self: Codable {
    var psqlType: PSQLDataType {
        .jsonb
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .jsonb:
            guard JSONBVersionByte == buffer.readInteger(as: UInt8.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return try context.jsonDecoder.decode(Self.self, from: buffer)
        case .json:
            return try context.jsonDecoder.decode(Self.self, from: buffer)
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        byteBuffer.writeInteger(JSONBVersionByte)
        try context.jsonEncoder.encode(self, into: &byteBuffer)
    }
}
