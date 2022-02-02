import NIOCore

extension Float: PSQLCodable {
    var psqlType: PSQLDataType {
        .float4
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Float {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.psqlReadFloat() else {
                throw PSQLCastingError.Code.failure
            }
            return float
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.psqlReadDouble() else {
                throw PSQLCastingError.Code.failure
            }
            return Float(double)
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Float(string) else {
                throw PSQLCastingError.Code.failure
            }
            return value
        default:
            throw PSQLCastingError.Code.typeMismatch
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.psqlWriteFloat(self)
    }
}

extension Double: PSQLCodable {
    var psqlType: PSQLDataType {
        .float8
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Double {
        switch (format, type) {
        case (.binary, .float4):
            guard buffer.readableBytes == 4, let float = buffer.psqlReadFloat() else {
                throw PSQLCastingError.Code.failure
            }
            return Double(float)
        case (.binary, .float8):
            guard buffer.readableBytes == 8, let double = buffer.psqlReadDouble() else {
                throw PSQLCastingError.Code.failure
            }
            return double
        case (.text, .float4), (.text, .float8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Double(string) else {
                throw PSQLCastingError.Code.failure
            }
            return value
        default:
            throw PSQLCastingError.Code.typeMismatch
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.psqlWriteDouble(self)
    }
}

