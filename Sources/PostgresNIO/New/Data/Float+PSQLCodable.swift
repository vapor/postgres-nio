import NIOCore

extension Float: PSQLCodable {
    public var psqlType: PSQLDataType {
        .float4
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Float {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.readFloat() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return float
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.readDouble() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Float(double)
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Float(string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeFloat(self)
    }
}

extension Double: PSQLCodable {
    public var psqlType: PSQLDataType {
        .float8
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Double {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.readFloat() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Double(float)
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.readDouble() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return double
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Double(string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeDouble(self)
    }
}

