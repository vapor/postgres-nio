import NIOCore

extension UInt8: PSQLCodable {
    public var psqlType: PSQLDataType {
        .char
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    // decoding
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .bpchar, .char:
            guard buffer.readableBytes == 1, let value = buffer.readInteger(as: UInt8.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: UInt8.self)
    }
}

extension Int16: PSQLCodable {
    
    public var psqlType: PSQLDataType {
        .int2
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    // decoding
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        case (.text, .int2):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int16(string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int16.self)
    }
}

extension Int32: PSQLCodable {
    public var psqlType: PSQLDataType {
        .int4
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    // decoding
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Int32(value)
        case (.binary, .int4):
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Int32(value)
        case (.text, .int2), (.text, .int4):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int32(string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int32.self)
    }
}

extension Int64: PSQLCodable {
    public var psqlType: PSQLDataType {
        .int8
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    // decoding
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Int64(value)
        case (.binary, .int4):
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Int64(value)
        case (.binary, .int8):
            guard buffer.readableBytes == 8, let value = buffer.readInteger(as: Int64.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        case (.text, .int2), (.text, .int4), (.text, .int8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int64(string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int64.self)
    }
}

extension Int: PSQLCodable {
    public var psqlType: PSQLDataType {
        switch self.bitWidth {
        case Int32.bitWidth:
            return .int4
        case Int64.bitWidth:
            return .int8
        default:
            preconditionFailure("Int is expected to be an Int32 or Int64")
        }
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    // decoding
    @inlinable
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Int(value)
        case (.binary, .int4):
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Int(value)
        case (.binary, .int8) where Int.bitWidth == 64:
            guard buffer.readableBytes == 8, let value = buffer.readInteger(as: Int.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        case (.text, .int2), (.text, .int4), (.text, .int8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int(string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    @inlinable
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int.self)
    }
}
