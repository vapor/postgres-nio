import NIOCore
import Foundation

/// A type that can encode itself to a postgres wire binary representation.
public protocol PostgresEncodable {
    /// identifies the data type that we will encode into `byteBuffer` in `encode`
    var psqlType: PostgresDataType { get }
    
    /// identifies the postgres format that is used to encode the value into `byteBuffer` in `encode`
    var psqlFormat: PostgresFormat { get }
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format, without setting
    /// the byte count. This method is called from the default `encodeRaw` implementation.
    func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) throws
}

/// A type that can decode itself from a postgres wire binary representation.
///
/// If you want to conform a type to PostgresDecodable you must implement the decode method.
public protocol PostgresDecodable {
    /// A type definition of the type that actually implements the PostgresDecodable protocol. This is an escape hatch to
    /// prevent a cycle in the conformace of the Optional type to PostgresDecodable.
    ///
    /// String? should be PostgresDecodable, String?? should not be PostgresDecodable
    associatedtype _DecodableType: PostgresDecodable = Self

    /// Decode an entity from the `byteBuffer` in postgres wire format
    ///
    /// - Parameters:
    ///   - byteBuffer: A `ByteBuffer` to decode. The byteBuffer is sliced in such a way that it is expected
    ///                 that the complete buffer is consumed for decoding
    ///   - type: The postgres data type. Depending on this type the `byteBuffer`'s bytes need to be interpreted
    ///           in different ways.
    ///   - format: The postgres wire format. Can be `.text` or `.binary`
    ///   - context: A `PostgresDecodingContext` providing context for decoding. This includes a `JSONDecoder`
    ///              to use when decoding json and metadata to create better errors.
    /// - Returns: A decoded object
    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self

    /// Decode an entity from the `byteBuffer` in postgres wire format. This method has a default implementation and
    /// is only overwritten for `Optional`s.
    static func _decodeRaw<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self
}

extension PostgresDecodable {
    @inlinable
    public static func _decodeRaw<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard var buffer = byteBuffer else {
            throw PostgresCastingError.Code.missingData
        }
        return try self.decode(from: &buffer, type: type, format: format, context: context)
    }
}

/// A type that can be encoded into and decoded from a postgres binary format
public protocol PostgresCodable: PostgresEncodable, PostgresDecodable {}

extension PostgresEncodable {
    public func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
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

public struct PostgresEncodingContext<JSONEncoder: PostgresJSONEncoder> {
    public let jsonEncoder: JSONEncoder

    public init(jsonEncoder: JSONEncoder) {
        self.jsonEncoder = jsonEncoder
    }
}

extension PostgresEncodingContext where JSONEncoder == Foundation.JSONEncoder {
    public static let `default` = PostgresEncodingContext(jsonEncoder: JSONEncoder())
}

public struct PostgresDecodingContext<JSONDecoder: PostgresJSONDecoder> {
    public let jsonDecoder: JSONDecoder
    
    init(jsonDecoder: JSONDecoder) {
        self.jsonDecoder = jsonDecoder
    }
}

extension PostgresDecodingContext where JSONDecoder == Foundation.JSONDecoder {
    public static let `default` = PostgresDecodingContext(jsonDecoder: Foundation.JSONDecoder())
}

extension Optional: PostgresDecodable where Wrapped: PostgresDecodable, Wrapped._DecodableType == Wrapped {
    public typealias _DecodableType = Wrapped

    public static func decode<JSONDecoder : PostgresJSONDecoder>(from byteBuffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PostgresDecodingContext<JSONDecoder>) throws -> Optional<Wrapped> {
        preconditionFailure("This should not be called")
    }

    @inlinable
    public static func _decodeRaw<JSONDecoder : PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer?,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Optional<Wrapped> {
        switch byteBuffer {
        case .some(var buffer):
            return try Wrapped.decode(from: &buffer, type: type, format: format, context: context)
        case .none:
            return .none
        }
    }
}
