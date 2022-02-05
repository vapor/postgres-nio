import NIOCore
import Foundation

/// A type that can encode itself to a postgres wire binary representation.
public protocol PSQLEncodable {
    /// identifies the data type that we will encode into `byteBuffer` in `encode`
    var psqlType: PSQLDataType { get }
    
    /// identifies the postgres format that is used to encode the value into `byteBuffer` in `encode`
    var psqlFormat: PSQLFormat { get }
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format, without setting
    /// the byte count. This method is called from the default `encodeRaw` implementation.
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws
    
    /// Encode the entity into the `byteBuffer` in Postgres binary format including its
    /// leading byte count. This method has a default implementation and may be overriden
    /// only for special cases, like `Optional`s.
    func encodeRaw(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws
}

/// A type that can decode itself from a postgres wire binary representation.
public protocol PSQLDecodable {
    typealias ActualType = Self

    /// Decode an entity from the `byteBuffer` in postgres wire format
    ///
    /// - Parameters:
    ///   - byteBuffer: A `ByteBuffer` to decode. The byteBuffer is sliced in such a way that it is expected
    ///                 that the complete buffer is consumed for decoding
    ///   - type: The postgres data type. Depending on this type the `byteBuffer`'s bytes need to be interpreted
    ///           in different ways.
    ///   - format: The postgres wire format. Can be `.text` or `.binary`
    ///   - context: A `PSQLDecodingContext` providing context for decoding. This includes a `JSONDecoder`
    ///              to use when decoding json and metadata to create better errors.
    /// - Returns: A decoded object
    static func decode<JSONDecoder: PSQLJSONDecoder>(from byteBuffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext<JSONDecoder>) throws -> Self

    /// Decode an entity from the `byteBuffer` in postgres wire format.
    /// This method has a default implementation and may be overriden
    /// only for special cases, like `Optional`s.
    static func decodeRaw<JSONDecoder: PSQLJSONDecoder>(from byteBuffer: inout ByteBuffer?, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext<JSONDecoder>) throws -> Self
}

extension PSQLDecodable {
    @inlinable
    public static func decodeRaw<JSONDecoder: PSQLJSONDecoder>(from byteBuffer: inout ByteBuffer?, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext<JSONDecoder>) throws -> Self {
        switch byteBuffer {
        case .some(var buffer):
            return try self.decode(from: &buffer, type: type, format: format, context: context)
        case .none:
            throw PSQLCastingError.Code.missingData
        }
    }
}

/// A type that can be encoded into and decoded from a postgres binary format
public protocol PSQLCodable: PSQLEncodable, PSQLDecodable {}

extension PSQLEncodable {
    @inlinable
    public func encodeRaw(into buffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
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

public struct PSQLEncodingContext {
    let jsonEncoder: PSQLJSONEncoder
}

public struct PSQLDecodingContext<Decoder: PSQLJSONDecoder> {
    
    public let jsonDecoder: Decoder
    
    init(jsonDecoder: Decoder) {
        self.jsonDecoder = jsonDecoder
    }
}

extension PSQLDecodingContext where Decoder == Foundation.JSONDecoder {
    static let `default` = PSQLDecodingContext(jsonDecoder: JSONDecoder())
}
