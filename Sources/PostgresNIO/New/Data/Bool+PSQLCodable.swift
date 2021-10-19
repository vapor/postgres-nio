import NIOCore

extension Bool: PSQLEncodable {
    public var psqlType: PSQLDataType {
        .bool
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    public func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}

extension Bool: PSQLDecodable {
    
    public static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Bool {
        guard type == .bool else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        switch format {
        case .binary:
            guard buffer.readableBytes == 1 else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            switch buffer.readInteger(as: UInt8.self) {
            case .some(0):
                return false
            case .some(1):
                return true
            default:
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
        case .text:
            guard buffer.readableBytes == 1 else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            switch buffer.readInteger(as: UInt8.self) {
            case .some(UInt8(ascii: "f")):
                return false
            case .some(UInt8(ascii: "t")):
                return true
            default:
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
        }
    }
}
