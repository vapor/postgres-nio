//
//  File.swift
//  
//
//  Created by Fabian Fett on 13.01.21.
//

extension PSQLCodable where Self: RawRepresentable, RawValue: PSQLCodable {
    var psqlType: PSQLDataType {
        self.rawValue.psqlType
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self {
                
        guard let rawValue = try? RawValue.decode(from: &buffer, type: type, context: context),
              let selfValue = Self.init(rawValue: rawValue) else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        return selfValue
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        try rawValue.encode(into: &byteBuffer, context: context)
    }
}
