import NIOCore
import struct Foundation.UUID
import typealias Foundation.uuid_t

extension UUID: PostgresEncodable {
    public static var psqlType: PostgresDataType {
        .uuid
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeUUIDBytes(self)
    }
}

extension UUID: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .uuid):
            guard let uuid = buffer.readUUIDBytes() else {
                throw PostgresDecodingError.Code.failure
            }
            self = uuid
        case (.binary, .varchar),
             (.binary, .text),
             (.text, .uuid),
             (.text, .text),
             (.text, .varchar):
            guard buffer.readableBytes == 36 else {
                throw PostgresDecodingError.Code.failure
            }

            guard let uuid = buffer.readString(length: 36).flatMap({ UUID(uuidString: $0) }) else {
                throw PostgresDecodingError.Code.failure
            }
            self = uuid
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}

extension UUID: PostgresCodable {}
