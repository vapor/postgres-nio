import NIOCore
import struct Foundation.UUID

// MARK: Protocols

/// A type, of which arrays can be encoded into and decoded from a postgres binary format
public protocol PostgresArrayEncodable: PostgresEncodable {
    static var psqlArrayType: PostgresDataType { get }
}

/// A type that can be decoded into a Swift Array of its own type from a Postgres array.
public protocol PostgresArrayDecodable: PostgresDecodable {}

// MARK: Element conformances

extension Bool: PostgresArrayDecodable {}

extension Bool: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .boolArray }
}

extension ByteBuffer: PostgresArrayDecodable {}

extension ByteBuffer: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .byteaArray }
}

extension UInt8: PostgresArrayDecodable {}

extension UInt8: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .charArray }
}


extension Int16: PostgresArrayDecodable {}

extension Int16: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .int2Array }
}

extension Int32: PostgresArrayDecodable {}

extension Int32: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .int4Array }
}

extension Int64: PostgresArrayDecodable {}

extension Int64: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .int8Array }
}

extension Int: PostgresArrayDecodable {}

extension Int: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType {
        if MemoryLayout<Int>.size == 8 {
            return .int8Array
        }
        return .int4Array
    }
}

extension Float: PostgresArrayDecodable {}

extension Float: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .float4Array }
}

extension Double: PostgresArrayDecodable {}

extension Double: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .float8Array }
}

extension String: PostgresArrayDecodable {}

extension String: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .textArray }
}

extension UUID: PostgresArrayDecodable {}

extension UUID: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .uuidArray }
}

extension Range: PostgresArrayDecodable where Bound: PostgresRangeArrayDecodable {}

extension Range: PostgresArrayEncodable where Bound: PostgresRangeArrayEncodable {
    public static var psqlArrayType: PostgresDataType { Bound.psqlRangeArrayType }
}

extension ClosedRange: PostgresArrayDecodable where Bound: PostgresRangeArrayDecodable {}

extension ClosedRange: PostgresArrayEncodable where Bound: PostgresRangeArrayEncodable {
    public static var psqlArrayType: PostgresDataType { Bound.psqlRangeArrayType }
}

// MARK: Array conformances

extension Array: PostgresEncodable where Element: PostgresArrayEncodable {
    public static var psqlType: PostgresDataType {
        Element.psqlArrayType
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        // 0 if empty, 1 if not
        buffer.writeInteger(self.isEmpty ? 0 : 1, as: UInt32.self)
        // b
        buffer.writeInteger(0, as: Int32.self)
        // array element type
        buffer.writeInteger(Element.psqlType.rawValue)

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

extension Array: PostgresNonThrowingEncodable where Element: PostgresArrayEncodable & PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType {
        Element.psqlArrayType
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        // 0 if empty, 1 if not
        buffer.writeInteger(self.isEmpty ? 0 : 1, as: UInt32.self)
        // b
        buffer.writeInteger(0, as: Int32.self)
        // array element type
        buffer.writeInteger(Element.psqlType.rawValue)

        // continue if the array is not empty
        guard !self.isEmpty else {
            return
        }

        // length of array
        buffer.writeInteger(numericCast(self.count), as: Int32.self)
        // dimensions
        buffer.writeInteger(1, as: Int32.self)

        self.forEach { element in
            element.encodeRaw(into: &buffer, context: context)
        }
    }
}


extension Array: PostgresDecodable where Element: PostgresArrayDecodable, Element == Element._DecodableType {
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard case .binary = format else {
            // currently we only support decoding arrays in binary format.
            throw PostgresDecodingError.Code.failure
        }

        guard let (isNotEmpty, b, element) = buffer.readMultipleIntegers(endianness: .big, as: (Int32, Int32, UInt32).self),
              0 <= isNotEmpty, isNotEmpty <= 1, b == 0
        else {
            throw PostgresDecodingError.Code.failure
        }

        let elementType = PostgresDataType(element)

        guard isNotEmpty == 1 else {
            self = []
            return
        }

        guard let (expectedArrayCount, dimensions) = buffer.readMultipleIntegers(endianness: .big, as: (Int32, Int32).self),
              expectedArrayCount > 0,
              dimensions == 1
        else {
            throw PostgresDecodingError.Code.failure
        }

        var result = Array<Element>()
        result.reserveCapacity(Int(expectedArrayCount))

        for _ in 0 ..< expectedArrayCount {
            guard let elementLength = buffer.readInteger(as: Int32.self), elementLength >= 0 else {
                throw PostgresDecodingError.Code.failure
            }

            guard var elementBuffer = buffer.readSlice(length: numericCast(elementLength)) else {
                throw PostgresDecodingError.Code.failure
            }

            let element = try Element.init(from: &elementBuffer, type: elementType, format: format, context: context)

            result.append(element)
        }

        self = result
    }
}
