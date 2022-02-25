import NIOCore
import struct Foundation.UUID

/// A type, of which arrays can be encoded into and decoded from a postgres binary format
public protocol PSQLArrayElement: PostgresCodable {
    static var psqlArrayType: PostgresDataType { get }
    static var psqlArrayElementType: PostgresDataType { get }
}

extension Bool: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .boolArray }
    public static var psqlArrayElementType: PostgresDataType { .bool }
}

extension ByteBuffer: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .byteaArray }
    public static var psqlArrayElementType: PostgresDataType { .bytea }
}

extension UInt8: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .charArray }
    public static var psqlArrayElementType: PostgresDataType { .char }
}

extension Int16: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .int2Array }
    public static var psqlArrayElementType: PostgresDataType { .int2 }
}

extension Int32: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .int4Array }
    public static var psqlArrayElementType: PostgresDataType { .int4 }
}

extension Int64: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .int8Array }
    public static var psqlArrayElementType: PostgresDataType { .int8 }
}

extension Int: PSQLArrayElement {
    #if (arch(i386) || arch(arm))
    static var psqlArrayType: PostgresDataType { .int4Array }
    static var psqlArrayElementType: PostgresDataType { .int4 }
    #else
    public static var psqlArrayType: PostgresDataType { .int8Array }
    public static var psqlArrayElementType: PostgresDataType { .int8 }
    #endif
}

extension Float: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .float4Array }
    public static var psqlArrayElementType: PostgresDataType { .float4 }
}

extension Double: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .float8Array }
    public static var psqlArrayElementType: PostgresDataType { .float8 }
}

extension String: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .textArray }
    public static var psqlArrayElementType: PostgresDataType { .text }
}

extension UUID: PSQLArrayElement {
    public static var psqlArrayType: PostgresDataType { .uuidArray }
    public static var psqlArrayElementType: PostgresDataType { .uuid }
}

extension Array: PostgresEncodable where Element: PSQLArrayElement {
    public var psqlType: PostgresDataType {
        Element.psqlArrayType
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        // 0 if empty, 1 if not
        buffer.writeInteger(self.isEmpty ? 0 : 1, as: UInt32.self)
        // b
        buffer.writeInteger(0, as: Int32.self)
        // array element type
        buffer.writeInteger(Element.psqlArrayElementType.rawValue)

        // continue if the array is not empty
        guard !self.isEmpty else {
            return
        }
        
        // length of array
        buffer.writeInteger(numericCast(self.count), as: Int32.self)
        // dimensions
        buffer.writeInteger(1, as: Int32.self)

        try self.forEach { element in
            try element.encodeRaw(into: &buffer, context: context)
        }
    }
}

extension Array: PostgresDecodable where Element: PSQLArrayElement {
    public static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard case .binary = format else {
            // currently we only support decoding arrays in binary format.
            throw PostgresCastingError.Code.failure
        }
        
        guard let (isNotEmpty, b, element) = buffer.readMultipleIntegers(endianness: .big, as: (Int32, Int32, UInt32).self),
              0 <= isNotEmpty, isNotEmpty <= 1, b == 0
        else {
            throw PostgresCastingError.Code.failure
        }
        
        let elementType = PostgresDataType(element)
        
        guard isNotEmpty == 1 else {
            return []
        }
        
        guard let (expectedArrayCount, dimensions) = buffer.readMultipleIntegers(endianness: .big, as: (Int32, Int32).self),
              expectedArrayCount > 0,
              dimensions == 1
        else {
            throw PostgresCastingError.Code.failure
        }
                
        var result = Array<Element>()
        result.reserveCapacity(Int(expectedArrayCount))
        
        for _ in 0 ..< expectedArrayCount {
            guard let elementLength = buffer.readInteger(as: Int32.self) else {
                throw PostgresCastingError.Code.failure
            }
            
            guard var elementBuffer = buffer.readSlice(length: numericCast(elementLength)) else {
                throw PostgresCastingError.Code.failure
            }
            
            let element = try Element.decode(from: &elementBuffer, type: elementType, format: format, context: context)
            
            result.append(element)
        }
        
        return result
    }
}

extension Array: PostgresCodable where Element: PSQLArrayElement {

}
