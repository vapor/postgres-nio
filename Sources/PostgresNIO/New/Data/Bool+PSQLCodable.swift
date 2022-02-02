import NIOCore

extension Bool: PSQLCodable {
    var psqlType: PSQLDataType {
        .bool
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard type == .bool else {
            throw PSQLCastingError.Code.typeMismatch
        }
        
        switch format {
        case .binary:
            guard buffer.readableBytes == 1 else {
                throw PSQLCastingError.Code.failure
            }
            
            switch buffer.readInteger(as: UInt8.self) {
            case .some(0):
                return false
            case .some(1):
                return true
            default:
                throw PSQLCastingError.Code.failure
            }
        case .text:
            guard buffer.readableBytes == 1 else {
                throw PSQLCastingError.Code.failure
            }
            
            switch buffer.readInteger(as: UInt8.self) {
            case .some(UInt8(ascii: "f")):
                return false
            case .some(UInt8(ascii: "t")):
                return true
            default:
                throw PSQLCastingError.Code.failure
            }
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}
