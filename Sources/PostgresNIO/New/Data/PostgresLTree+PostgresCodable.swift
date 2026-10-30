/// A Swift representation for the Postgres `ltree` type, provided by the `ltree` extension.
///
/// An `ltree` value is a sequence of zero or more labels separated by dots, for example
/// `Top.Science.Astronomy`.
///
/// Enable the extension in your database before using the type:
/// ```sql
/// CREATE EXTENSION IF NOT EXISTS ltree;
/// ```
///
/// ## Wire format
///
/// Because `ltree` is an extension type its Postgres OID is assigned when the extension is installed
/// and is therefore not stable across databases. It is instead resolved per connection: add `"ltree"`
/// to ``PostgresConnection/Configuration/Options/additionalDataTypeNames`` and the OID is looked up
/// while the connection is being established. The dotted path is always written as text, never as binary.
///
/// ```swift
/// config.options.additionalDataTypeNames = ["ltree"]
/// ```
///
/// Without that option, or if the extension is not installed, values are bound with an unspecified
/// parameter type (OID `0`) and Postgres has to infer the type from context. Binding an `ltree` where
/// Postgres can't infer it (for example `SELECT $1` with no surrounding type information)
/// will then fail.
///
/// When decoding, both wire formats the server may send are supported:
/// - `.text`: the raw dotted path.
/// - `.binary`: a one-byte version prefix (currently `1`) followed by the path text. Binary
///   send/receive for `ltree` was only added in PostgreSQL 13.
///
/// For more information on the type, see https://www.postgresql.org/docs/current/ltree.html.
public struct PostgresLTree: Equatable, Hashable, Sendable {
    @usableFromInline var values: [String]

    /// Create an `ltree` from its individual labels, in order from the root to the leaf.
    ///
    /// - Parameter values: The labels making up the path, e.g. `["Top", "Science", "Astronomy"]`.
    public init(labels: [String]) {
        self.values = labels
    }

    /// Append a label to the end (leaf) of the path.
    public mutating func append(_ label: String) {
        values.append(label)
    }

    /// Remove and return the last (leaf) label of the path, or `nil` if the path is empty.
    public mutating func popLast() -> String? {
        values.popLast()
    }

    /// The individual labels of the path, ordered from the root to the leaf.
    public var labels: [String] {
        self.values
    }
}

extension PostgresLTree: ExpressibleByArrayLiteral {
    /// Create an `ltree` from its individual labels, in order from the root to the leaf.
    ///
    /// - Parameter arrayLiteral: The labels making up the path, e.g. `["Top", "Science", "Astronomy"]`.
    public init(arrayLiteral elements: String...) {
        self.values = elements
    }
}

extension PostgresLTree: CustomStringConvertible, LosslessStringConvertible {
    /// Create an `ltree` by splitting a dotted path string into its labels.
    /// 
    /// Input that Postgres would reject (such as `a..b`) is parsed leniently
    /// rather than failing. The server is the authority here.
    ///
    /// - Parameter description: A dotted path such as `Top.Science.Astronomy`.
    public init(_ description: String) {
        self.values = description.split(separator: ".").map(String.init)
    }

    /// The path as a dotted string, e.g. `Top.Science.Astronomy`.
    public var description: String {
        self.values.joined(separator: ".")
    }
}

extension PostgresLTree: PostgresDecodable {
    /// Decode an `ltree` value from either wire format.
    @inlinable
    public init<JSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws where JSONDecoder: PostgresJSONDecoder {
        if format == .binary {
            let version = byteBuffer.readInteger(as: Int8.self)
            guard version == 1 else { throw PostgresDecodingError.Code.failure }
        }

        let string = byteBuffer.readString(length: byteBuffer.readableBytes)
        guard let string else { throw PostgresDecodingError.Code.failure }
        self = PostgresLTree(string)
    }
}

extension PostgresLTree: PostgresDynamicTypeEncodable {
    public var psqlType: PostgresDataType {
        .null  // unspecified, since ltree's OID is not stable across databases
    }

    public var psqlFormat: PostgresFormat {
        .text
    }

    public var psqlTypeName: String? {
        "ltree"
    }

    @inlinable
    public func encode<JSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        // Write directly into the buffer avoiding extra allocations of `.joined`
        for (index, label) in self.values.enumerated() {
            if index != 0 {
                byteBuffer.writeInteger(UInt8(ascii: "."))
            }
            byteBuffer.writeString(label)
        }
    }
}
