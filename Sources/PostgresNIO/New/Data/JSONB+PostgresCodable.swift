// MARK: - PostgresJSONB wrapper

/// A wrapper that encodes any `Encodable` value as a Postgres JSONB bind parameter.
///
/// Use this to bind values that don't conform to any of the Postgres encoding protocols
/// (``PostgresEncodable``, ``PostgresDynamicTypeEncodable``, etc.) directly as JSONB.
///
/// ```swift
/// struct MyModel: Codable { var name: String; var tags: [String] }
/// let model = MyModel(name: "example", tags: ["a", "b"])
///
/// // Via the wrapper type directly:
/// try bindings.append(PostgresJSONB(model))
///
/// // Via string interpolation convenience:
/// let query: PostgresQuery = "INSERT INTO t (data) VALUES (\(jsonb: model))"
/// ```
public struct PostgresJSONB<Value: Encodable>: PostgresEncodable, PostgresThrowingDynamicTypeEncodable, Sendable
    where Value: Sendable
{
    public static var psqlType: PostgresDataType { .jsonb }
    public static var psqlFormat: PostgresFormat { .binary }

    @usableFromInline
    let value: Value

    /// Wrap an `Encodable & Sendable` value for binding as JSONB.
    @inlinable
    public init(_ value: Value) {
        self.value = value
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        byteBuffer.writeInteger(JSONBVersionByte)
        try context.jsonEncoder.encode(self.value, into: &byteBuffer)
    }
}

// MARK: - PostgresJSONBArray wrapper

/// A wrapper that encodes an array of `Encodable` values as a native Postgres `JSONB[]` array,
/// where each element is individually JSONB-encoded.
///
/// This is distinct from ``PostgresJSONB`` wrapping an array, which would produce a single JSONB
/// value containing a JSON array. `PostgresJSONBArray` produces a Postgres array whose element type
/// is `jsonb` — i.e. each element is a separate JSONB datum within the array wire format.
///
/// ```swift
/// struct Tag: Codable { var label: String }
/// let tags: [Tag] = [Tag(label: "swift"), Tag(label: "postgres")]
///
/// // Produces a JSONB[] bind:
/// try bindings.append(PostgresJSONBArray(tags))
///
/// // Via string interpolation convenience:
/// let query: PostgresQuery = "INSERT INTO t (tags) VALUES (\(jsonb: tags))"
/// ```
public struct PostgresJSONBArray<Element: Encodable>: PostgresEncodable, PostgresThrowingDynamicTypeEncodable, Sendable
    where Element: Sendable
{
    public static var psqlType: PostgresDataType { .jsonbArray }
    public static var psqlFormat: PostgresFormat { .binary }

    @usableFromInline
    let elements: [Element]

    /// Wrap an array of `Encodable & Sendable` values for binding as a Postgres `JSONB[]` array.
    @inlinable
    public init(_ elements: [Element]) {
        self.elements = elements
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        // Postgres array binary format header
        // dimensions: 0 if empty, 1 if not
        byteBuffer.writeInteger(self.elements.isEmpty ? 0 : 1, as: UInt32.self)
        // has-null flag (we never write NULLs; nullable elements would need Optional handling)
        byteBuffer.writeInteger(0, as: Int32.self)
        // element OID
        byteBuffer.writeInteger(PostgresDataType.jsonb.rawValue)

        guard !self.elements.isEmpty else {
            return
        }

        // length of the single dimension
        byteBuffer.writeInteger(numericCast(self.elements.count), as: Int32.self)
        // lower bound (1-based, standard Postgres convention)
        byteBuffer.writeInteger(1, as: Int32.self)

        for element in self.elements {
            // Reserve space for the element byte-length prefix
            let lengthIndex = byteBuffer.writerIndex
            byteBuffer.writeInteger(0, as: Int32.self)
            let startIndex = byteBuffer.writerIndex

            // Write the JSONB payload (version byte + JSON data)
            byteBuffer.writeInteger(JSONBVersionByte)
            try context.jsonEncoder.encode(element, into: &byteBuffer)

            // Patch the length
            byteBuffer.setInteger(
                Int32(byteBuffer.writerIndex - startIndex),
                at: lengthIndex
            )
        }
    }
