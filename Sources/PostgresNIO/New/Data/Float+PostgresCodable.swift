import NIOCore

extension Float: PostgresCodable {
    public var psqlType: PostgresDataType {
        .float4
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.psqlReadFloat() else {
                throw PostgresCastingError.Code.failure
            }
            return float
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.psqlReadDouble() else {
                throw PostgresCastingError.Code.failure
            }
            return Float(double)
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Float(string) else {
                throw PostgresCastingError.Code.failure
            }
            return value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.psqlWriteFloat(self)
    }
}

extension Double: PostgresCodable {
    public var psqlType: PostgresDataType {
        .float8
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }
    
    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.psqlReadFloat() else {
                throw PostgresCastingError.Code.failure
            }
            return Double(float)
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.psqlReadDouble() else {
                throw PostgresCastingError.Code.failure
            }
            return double
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Double(string) else {
                throw PostgresCastingError.Code.failure
            }
            return value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.psqlWriteDouble(self)
    }
}

