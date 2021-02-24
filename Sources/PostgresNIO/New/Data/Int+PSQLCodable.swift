extension UInt8: PSQLCodable {
    var psqlType: PSQLDataType {
        .char
    }
    
    // decoding
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
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
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: UInt8.self)
    }
}

extension Int16: PSQLCodable {
    
    var psqlType: PSQLDataType {
        .int2
    }
    
    // decoding
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .int2:
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int16.self)
    }
}

extension Int32: PSQLCodable {
    var psqlType: PSQLDataType {
        .int4
    }
    
    // decoding
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .int2:
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return Int32(value)
        case .int4:
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return Int32(value)
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int32.self)
    }
}

extension Int64: PSQLCodable {
    var psqlType: PSQLDataType {
        .int8
    }
    
    // decoding
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .int2:
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return Int64(value)
        case .int4:
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return Int64(value)
        case .int8:
            guard buffer.readableBytes == 8, let value = buffer.readInteger(as: Int64.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int64.self)
    }
}

extension Int: PSQLCodable {
    var psqlType: PSQLDataType {
        switch self.bitWidth {
        case Int32.bitWidth:
            return .int4
        case Int64.bitWidth:
            return .int8
        default:
            preconditionFailure("Int is expected to be an Int32 or Int64")
        }
    }
    
    // decoding
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .int2:
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return Int(value)
        case .int4:
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return Int(value)
        case .int8 where Int.bitWidth == 64:
            guard buffer.readableBytes == 8, let value = buffer.readInteger(as: Int.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    // encoding
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self, as: Int.self)
    }
}
