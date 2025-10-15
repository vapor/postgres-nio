import NIOCore
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

/// A type that can encode itself to a Postgres wire binary representation.
/// Dynamic types are types that don't have a well-known Postgres type OID at compile time.
/// For example, custom types created at runtime, such as enums, or extension types whose OID is not stable between
/// databases.
public protocol PostgresThrowingDynamicTypeEncodable {
    /// The data type encoded into the `byteBuffer` in ``encode(into:context:)``
    var psqlType: PostgresDataType { get }

    /// The Postgres encoding format used to encode the value into `byteBuffer` in ``encode(into:context:)``.
    var psqlFormat: PostgresFormat { get }

    /// Encode the entity into ``byteBuffer`` in the format specified by ``psqlFormat``,
    /// using the provided ``context`` as needed, without setting the byte count.
    ///
    /// This method is called by ``PostgresBindings``.
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws
}

/// A type that can encode itself to a Postgres wire binary representation.
/// Dynamic types are types that don't have a well-known Postgres type OID at compile time.
/// For example, custom types created at runtime, such as enums, or extension types whose OID is not stable between
/// databases.
///
/// This is the non-throwing alternative to ``PostgresThrowingDynamicTypeEncodable``. It allows users
/// to create ``PostgresQuery``s via `ExpressibleByStringInterpolation` without having to spell `try`.
public protocol PostgresDynamicTypeEncodable: PostgresThrowingDynamicTypeEncodable {
    /// Encode the entity into ``byteBuffer`` in the format specified by ``psqlFormat``,
    /// using the provided ``context`` as needed, without setting the byte count.
    ///
    /// This method is called by ``PostgresBindings``.
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    )
}

/// A type that can encode itself to a postgres wire binary representation.
///
/// Conform your custom types to `PostgresEncodable` to enable them to be used as query parameters
/// with `PostgresQuery` and `PostgresBindings`.
///
/// ## Conforming Built-in Types
///
/// Many standard Swift types already conform to `PostgresEncodable`:
/// - Numeric types: `Int`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt`, `UInt8`, `UInt16`, `UInt32`, `Float`, `Double`
/// - Text types: `String`, `Substring`
/// - Other types: `Bool`, `Date`, `UUID`, `Data`
/// - Collections: `Array` (where Element is encodable)
///
/// ## Implementing PostgresEncodable
///
/// To make a custom type encodable, implement the protocol requirements:
///
/// ```swift
/// import NIOCore
///
/// struct Point: PostgresEncodable {
///     let x: Double
///     let y: Double
///
///     static var psqlType: PostgresDataType { .point }
///     static var psqlFormat: PostgresFormat { .binary }
///
///     func encode<JSONEncoder: PostgresJSONEncoder>(
///         into buffer: inout ByteBuffer,
///         context: PostgresEncodingContext<JSONEncoder>
///     ) throws {
///         buffer.writeDouble(x)
///         buffer.writeDouble(y)
///     }
/// }
/// ```
///
/// ## Using Custom Encodable Types
///
/// Once your type conforms to `PostgresEncodable`, use it in queries:
///
/// ```swift
/// let point = Point(x: 10.5, y: 20.3)
/// let query: PostgresQuery = "INSERT INTO locations (coordinate) VALUES (\(point))"
/// ```
///
/// ## Encoding as JSON
///
/// For complex types, you can encode them as JSONB:
///
/// ```swift
/// struct User: Codable, PostgresEncodable {
///     let name: String
///     let email: String
///     let age: Int
///
///     static var psqlType: PostgresDataType { .jsonb }
///     static var psqlFormat: PostgresFormat { .binary }
///
///     func encode<JSONEncoder: PostgresJSONEncoder>(
///         into buffer: inout ByteBuffer,
///         context: PostgresEncodingContext<JSONEncoder>
///     ) throws {
///         // JSONB format version byte
///         buffer.writeInteger(1, as: UInt8.self)
///         // Encode as JSON using the provided encoder
///         let data = try context.jsonEncoder.encode(self)
///         buffer.writeBytes(data)
///     }
/// }
/// ```
///
/// ## RawRepresentable Types
///
/// For enums backed by encodable raw values:
///
/// ```swift
/// enum Status: String, PostgresEncodable {
///     case active
///     case inactive
///     case pending
///
///     static var psqlType: PostgresDataType { .text }
///     static var psqlFormat: PostgresFormat { .binary }
///
///     func encode<JSONEncoder: PostgresJSONEncoder>(
///         into buffer: inout ByteBuffer,
///         context: PostgresEncodingContext<JSONEncoder>
///     ) throws {
///         try rawValue.encode(into: &buffer, context: context)
///     }
/// }
/// ```
///
/// - Note: `PostgresNonThrowingEncodable` is a variant that doesn't throw, allowing usage without `try`.
/// - SeeAlso: `PostgresDecodable` for decoding values from Postgres.
public protocol PostgresEncodable: PostgresThrowingDynamicTypeEncodable {
    // TODO: Rename to `PostgresThrowingEncodable` with next major release

    /// The data type encoded into the `byteBuffer` in ``encode(into:context:)``.
    static var psqlType: PostgresDataType { get }

    /// The Postgres encoding format used to encode the value into `byteBuffer` in ``encode(into:context:)``.
    static var psqlFormat: PostgresFormat { get }
}

/// A type that can encode itself to a postgres wire binary representation. It enforces that the
/// ``PostgresEncodable/encode(into:context:)-1jkcp`` does not throw. This allows users
/// to create ``PostgresQuery``s via `ExpressibleByStringInterpolation` without
/// having to spell `try`.
public protocol PostgresNonThrowingEncodable: PostgresEncodable, PostgresDynamicTypeEncodable {
    // TODO: Rename to `PostgresEncodable` with next major release
}

/// A type that can decode itself from a postgres wire binary representation.
///
/// Conform your custom types to `PostgresDecodable` to enable them to be decoded from query results.
///
/// ## Conforming Built-in Types
///
/// Many standard Swift types already conform to `PostgresDecodable`:
/// - Numeric types: `Int`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt`, `UInt8`, `UInt16`, `UInt32`, `Float`, `Double`
/// - Text types: `String`, `Substring`
/// - Other types: `Bool`, `Date`, `UUID`, `Data`
/// - Collections: `Array` (where Element is decodable)
///
/// ## Implementing PostgresDecodable
///
/// To make a custom type decodable, implement the required initializer:
///
/// ```swift
/// import NIOCore
///
/// struct Point: PostgresDecodable {
///     let x: Double
///     let y: Double
///
///     init<JSONDecoder: PostgresJSONDecoder>(
///         from buffer: inout ByteBuffer,
///         type: PostgresDataType,
///         format: PostgresFormat,
///         context: PostgresDecodingContext<JSONDecoder>
///     ) throws {
///         guard type == .point else {
///             throw PostgresDecodingError.Code.typeMismatch
///         }
///         guard let x = buffer.readDouble(), let y = buffer.readDouble() else {
///             throw PostgresDecodingError.Code.missingData
///         }
///         self.x = x
///         self.y = y
///     }
/// }
/// ```
///
/// ## Using Custom Decodable Types
///
/// Once your type conforms to `PostgresDecodable`, decode it from query results:
///
/// ```swift
/// let rows = try await connection.query("SELECT coordinate FROM locations", logger: logger)
/// for try await row in rows {
///     let point = try row.decode(Point.self, context: .default)
///     print("Point: (\(point.x), \(point.y))")
/// }
/// ```
///
/// ## Decoding from JSON
///
/// For complex types stored as JSON or JSONB:
///
/// ```swift
/// struct User: Codable, PostgresDecodable {
///     let name: String
///     let email: String
///     let age: Int
///
///     init<JSONDecoder: PostgresJSONDecoder>(
///         from buffer: inout ByteBuffer,
///         type: PostgresDataType,
///         format: PostgresFormat,
///         context: PostgresDecodingContext<JSONDecoder>
///     ) throws {
///         guard type == .jsonb || type == .json else {
///             throw PostgresDecodingError.Code.typeMismatch
///         }
///
///         var jsonBuffer = buffer
///         if type == .jsonb {
///             // Skip JSONB version byte
///             _ = jsonBuffer.readInteger(as: UInt8.self)
///         }
///
///         guard let data = jsonBuffer.readData(length: jsonBuffer.readableBytes) else {
///             throw PostgresDecodingError.Code.missingData
///         }
///
///         self = try context.jsonDecoder.decode(User.self, from: data)
///     }
/// }
/// ```
///
/// ## Decoding RawRepresentable Types
///
/// For enums backed by decodable raw values:
///
/// ```swift
/// enum Status: String, PostgresDecodable {
///     case active
///     case inactive
///     case pending
///
///     init<JSONDecoder: PostgresJSONDecoder>(
///         from buffer: inout ByteBuffer,
///         type: PostgresDataType,
///         format: PostgresFormat,
///         context: PostgresDecodingContext<JSONDecoder>
///     ) throws {
///         let rawValue = try String(from: &buffer, type: type, format: format, context: context)
///         guard let value = Self(rawValue: rawValue) else {
///             throw PostgresDecodingError.Code.failure
///         }
///         self = value
///     }
/// }
/// ```
///
/// ## Handling Optional Values
///
/// Optional values are automatically handled by the protocol:
///
/// ```swift
/// let email: String? = try row.decode(String?.self, context: .default)
/// // Returns nil if the database value is NULL
/// ```
///
/// - Note: The `_DecodableType` associated type is an implementation detail for Optional handling.
/// - SeeAlso: `PostgresEncodable` for encoding values to Postgres.
public protocol PostgresDecodable {
    /// A type definition of the type that actually implements the PostgresDecodable protocol. This is an escape hatch to
    /// prevent a cycle in the conformace of the Optional type to PostgresDecodable.
    ///
    /// String? should be PostgresDecodable, String?? should not be PostgresDecodable
    associatedtype _DecodableType: PostgresDecodable = Self

    /// Create an entity from the `byteBuffer` in postgres wire format
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
    init<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws

    /// Decode an entity from the `byteBuffer` in postgres wire format. This method has a default implementation and
    /// is only overwritten for `Optional`s. Other than in the
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
            throw PostgresDecodingError.Code.missingData
        }
        return try self.init(from: &buffer, type: type, format: format, context: context)
    }
}

/// A type that can be encoded into and decoded from a postgres binary format
public typealias PostgresCodable = PostgresEncodable & PostgresDecodable

extension PostgresEncodable {
    @inlinable
    public var psqlType: PostgresDataType { Self.psqlType }

    @inlinable
    public var psqlFormat: PostgresFormat { Self.psqlFormat }
}

extension PostgresThrowingDynamicTypeEncodable {
    @inlinable
    func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
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

extension PostgresDynamicTypeEncodable {
    @inlinable
    func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        // The length of the parameter value, in bytes (this count does not include
        // itself). Can be zero.
        let lengthIndex = buffer.writerIndex
        buffer.writeInteger(0, as: Int32.self)
        let startIndex = buffer.writerIndex
        // The value of the parameter, in the format indicated by the associated format
        // code. n is the above length.
        self.encode(into: &buffer, context: context)

        // overwrite the empty length, with the real value
        buffer.setInteger(numericCast(buffer.writerIndex - startIndex), at: lengthIndex, as: Int32.self)
    }
}

/// A context that is passed to Swift objects that are encoded into the Postgres wire format. Used
/// to pass further information to the encoding method.
public struct PostgresEncodingContext<JSONEncoder: PostgresJSONEncoder>: Sendable {
    /// A ``PostgresJSONEncoder`` used to encode the object to json.
    public var jsonEncoder: JSONEncoder

    /// Creates a ``PostgresEncodingContext`` with the given ``PostgresJSONEncoder``. In case you want
    /// to use the a ``PostgresEncodingContext`` with an unconfigured Foundation `JSONEncoder`
    /// you can use the ``default`` context instead.
    ///
    /// - Parameter jsonEncoder: A ``PostgresJSONEncoder`` to use when encoding objects to json
    public init(jsonEncoder: JSONEncoder) {
        self.jsonEncoder = jsonEncoder
    }
}

extension PostgresEncodingContext where JSONEncoder == Foundation.JSONEncoder {
    /// A default ``PostgresEncodingContext`` that uses a Foundation `JSONEncoder`.
    public static let `default` = PostgresEncodingContext(jsonEncoder: JSONEncoder())
}

/// A context that is passed to Swift objects that are decoded from the Postgres wire format. Used
/// to pass further information to the decoding method.
public struct PostgresDecodingContext<JSONDecoder: PostgresJSONDecoder>: Sendable {
    /// A ``PostgresJSONDecoder`` used to decode the object from json.
    public var jsonDecoder: JSONDecoder

    /// Creates a ``PostgresDecodingContext`` with the given ``PostgresJSONDecoder``. In case you want
    /// to use the a ``PostgresDecodingContext`` with an unconfigured Foundation `JSONDecoder`
    /// you can use the ``default`` context instead.
    ///
    /// - Parameter jsonDecoder: A ``PostgresJSONDecoder`` to use when decoding objects from json
    public init(jsonDecoder: JSONDecoder) {
        self.jsonDecoder = jsonDecoder
    }
}

extension PostgresDecodingContext where JSONDecoder == Foundation.JSONDecoder {
    /// A default ``PostgresDecodingContext`` that uses a Foundation `JSONDecoder`.
    public static let `default` = PostgresDecodingContext(jsonDecoder: Foundation.JSONDecoder())
}

extension Optional: PostgresDecodable where Wrapped: PostgresDecodable, Wrapped._DecodableType == Wrapped {
    public typealias _DecodableType = Wrapped

    public init<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
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
            return try Wrapped(from: &buffer, type: type, format: format, context: context)
        case .none:
            return .none
        }
    }
}
