import NIOCore

extension PostgresDataType: PostgresDecodable {
    @inlinable
    public init<JSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws where JSONDecoder: PostgresJSONDecoder {
        switch (format, type) {
        case (.binary, .oid), (.binary, .regproc), (.binary, .regclass), (.binary, .regtype):
            guard
                buffer.readableBytes == 4,
                let value = buffer.readInteger(as: UInt32.self)
            else {
                throw PostgresDecodingError.Code.failure
            }
            self.rawValue = value
        case (.binary, .int4):
            guard
                buffer.readableBytes == 4,
                let value = buffer.readInteger(as: Int32.self).flatMap(UInt32.init(exactly:))
            else {
                throw PostgresDecodingError.Code.failure
            }
            self.rawValue = value
        // The `oid` aliases (`regproc`, `regclass` etc) only carry an id in binary form.
        // In text form they're a string containing a name and so cannot be decoded into a PostgresDataType.
        case (.text, .oid), (.text, .int4):
            guard
                let string = buffer.readString(length: buffer.readableBytes),
                let value = UInt32(string)
            else {
                throw PostgresDecodingError.Code.failure
            }
            self.rawValue = value
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}

extension PostgresDataType: PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType {
        .oid
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self.rawValue, as: UInt32.self)
    }
}
