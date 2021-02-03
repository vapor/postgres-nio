//
//  File.swift
//  
//
//  Created by Fabian Fett on 12.01.21.
//

extension Bool: PSQLCodable {
    var psqlType: PSQLDataType {
        .bool
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Bool {
        guard type == .bool, buffer.readableBytes == 1 else {
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
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}
