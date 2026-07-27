import NIOCore

extension PostgresDataType: PostgresDecodable {
    @inlinable
    public init<JSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws where JSONDecoder: PostgresJSONDecoder {
        switch (format, type) {
        case (.binary, .oid), (.binary, .regproc):
            guard 
                byteBuffer.readableBytes == 4,
                let value = byteBuffer.readInteger(as: UInt32.self)
            else { 
                throw PostgresDecodingError.Code.failure 
            }
            self.rawValue = value
        case (.binary, .int4):
            guard 
                byteBuffer.readableBytes == 4,
                let value = byteBuffer.readInteger(as: Int32.self).flatMap(UInt32.init(exactly:))
            else { 
                throw PostgresDecodingError.Code.failure 
            }
            self.rawValue = value
        case (.text, .oid), (.text, .int4):
            guard 
                let string = byteBuffer.readString(length: byteBuffer.readableBytes),
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
        byteBuffer.writeInteger(self.rawValue)
    }
}
