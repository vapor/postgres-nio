extension Float: PSQLCodable {
    var psqlType: PSQLDataType {
        .float4
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Float {
        switch type {
        case .float4:
            guard buffer.readableBytes == 4, let float = buffer.readFloat() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return float
        case .float8:
            guard buffer.readableBytes == 8, let double = buffer.readDouble() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Float(double)
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeFloat(self)
    }
}

extension Double: PSQLCodable {
    var psqlType: PSQLDataType {
        .float8
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Double {
        switch type {
        case .float4:
            guard buffer.readableBytes == 4, let float = buffer.readFloat() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return Double(float)
        case .float8:
            guard buffer.readableBytes == 8, let double = buffer.readDouble() else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            return double
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeDouble(self)
    }
}

