import NIOCore
import struct Foundation.UUID

extension String: PostgresCodable {
    public var psqlType: PostgresDataType {
        .text
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeString(self)
    }

    @inlinable
    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> String {
        switch (format, type) {
        case (_, .varchar),
             (_, .text),
             (_, .name):
            // we can force unwrap here, since this method only fails if there are not enough
            // bytes available.
            return buffer.readString(length: buffer.readableBytes)!
        case (_, .uuid):
            guard let uuid = try? UUID.decode(from: &buffer, type: .uuid, format: format, context: context) else {
                throw PostgresCastingError.Code.failure
            }
            return uuid.uuidString
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}
