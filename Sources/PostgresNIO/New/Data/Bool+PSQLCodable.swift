import NIOCore

extension Bool: PSQLCodable {
    public var psqlType: PSQLDataType {
        .bool
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    public static func decode<JSONDecoder : PSQLJSONDecoder>(
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
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) {
        buffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}
