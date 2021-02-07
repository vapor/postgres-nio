import struct Foundation.UUID

extension String: PSQLCodable {
    var psqlType: PSQLDataType {
        .text
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeString(self)
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> String {
        switch type {
        case .varchar, .text, .name:
            // we can force unwrap here, since this method only fails if there are not enough
            // bytes available.
            return buffer.readString(length: buffer.readableBytes)!
        case .uuid:
            guard let uuid = try? UUID.decode(from: &buffer, type: .uuid, context: context) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return uuid.uuidString
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
}
