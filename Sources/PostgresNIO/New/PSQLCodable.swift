//
//  File.swift
//  
//
//  Created by Fabian Fett on 11.01.21.
//

/// A type that can encode itself to a postgres wire binary representation.
protocol PSQLEncodable {
    /// identifies the data type that we will encode into `byteBuffer` in `encode`
    var psqlType: PSQLDataType { get }
    
    /// encoding the entity into the `byteBuffer` in postgres binary format
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws
}

/// A type that can decode itself from a postgres wire binary representation.
protocol PSQLDecodable {

    /// decode an entity from the `byteBuffer` in postgres binary format
    static func decode(from byteBuffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> Self
}

/// A type that can be encoded into and decoded from a postgres binary format
protocol PSQLCodable: PSQLEncodable, PSQLDecodable {}

extension PSQLEncodable {
    func _encode(into buffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        // The length of the parameter value, in bytes (this count does not include
        // itself). Can be zero.
        let lengthIndex = buffer.writerIndex
        buffer.writeInteger(0, as: Int32.self)
        let startIndex = buffer.writerIndex
        // The value of the parameter, in the format indicated by the associated format
        // code. n is the above length.
        try self.encode(into: &buffer, context: context)
        
        // overwrite the empty length, with the real value
        buffer.setInteger(numericCast(buffer.writerIndex - startIndex), at: lengthIndex, as: Int32.self)
    }
}

struct PSQLEncodingContext {
    let jsonEncoder: PSQLJSONEncoder
}

struct PSQLDecodingContext {
    
    let jsonDecoder: PSQLJSONDecoder
    
    let columnIndex: Int
    let columnName: String
    
    let file: String
    let line: Int
    
    init(jsonDecoder: PSQLJSONDecoder, columnName: String, columnIndex: Int, file: String, line: Int) {
        self.jsonDecoder = jsonDecoder
        self.columnName = columnName
        self.columnIndex = columnIndex
        
        self.file = file
        self.line = line
    }
}
