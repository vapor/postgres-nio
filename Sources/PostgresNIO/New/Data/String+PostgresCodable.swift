import NIOCore
import struct Foundation.UUID

extension String: PostgresEncodable {
    public static var psqlType: PostgresDataType {
        .text
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeString(self)
    }
}

extension String: PostgresDecodable {

    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (_, .varchar),
             (_, .text),
             (_, .name):
            // we can force unwrap here, since this method only fails if there are not enough
            // bytes available.
            self = buffer.readString(length: buffer.readableBytes)!
        case (_, .uuid):
            guard let uuid = try? UUID(from: &buffer, type: .uuid, format: format, context: context) else {
                throw PostgresDecodingError.Code.failure
            }
            self = uuid.uuidString
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}
