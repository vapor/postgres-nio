import NIOCore

extension Bool: PSQLCodable {
    public var psqlType: PostgresDataType {
        .bool
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
        guard type == .bool else {
            throw PostgresCastingError.Code.typeMismatch
        }
        
        switch format {
        case .binary:
            guard buffer.readableBytes == 1 else {
                throw PostgresCastingError.Code.failure
            }
            
            switch buffer.readInteger(as: UInt8.self) {
            case .some(0):
                return false
            case .some(1):
                return true
            default:
                throw PostgresCastingError.Code.failure
            }
        case .text:
            guard buffer.readableBytes == 1 else {
                throw PostgresCastingError.Code.failure
            }
            
            switch buffer.readInteger(as: UInt8.self) {
            case .some(UInt8(ascii: "f")):
                return false
            case .some(UInt8(ascii: "t")):
                return true
            default:
                throw PostgresCastingError.Code.failure
            }
        }
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) {
        buffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}
