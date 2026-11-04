/// A Swift representation of the Postgres `ltree` type, which comes from the `ltree` extension.
///
/// An `ltree` is a path of labels separated by dots, e.g. `Top.Science.Astronomy`.
/// The extension needs to be enabled in the database before the type can be used:
/// ```sql
/// CREATE EXTENSION IF NOT EXISTS ltree;
/// ```
///
/// Since the type comes from an extension it doesn't have a fixed OID, so each connection looks it up
/// the first time it's used. If the extension is dropped and created again, connections that already
/// used the type keep the old OID and need to be reconnected.
///
/// To skip the lookup, bind the path as a `String` and cast it in SQL instead:
/// ```swift
/// try await client.query("SELECT \(path.description)::ltree")
/// ```
///
/// Values are always sent as text, and can be decoded from text or from binary, which Postgres supports
/// for `ltree` since version 13.
///
/// For more information see https://www.postgresql.org/docs/current/ltree.html.
public struct PostgresLTree: Equatable, Hashable, Sendable {
    /// The labels of the path, from the root to the leaf.
    ///
    /// A label can't contain a `.`, otherwise Postgres will split it into multiple labels.
    public var labels: [String]

    /// Create an `ltree` from its labels, from the root to the leaf.
    ///
    /// - Parameter labels: The labels making up the path, e.g. `["Top", "Science", "Astronomy"]`.
    public init(labels: [String]) {
        self.labels = labels
    }
}

extension PostgresLTree: ExpressibleByArrayLiteral {
    /// Create an `ltree` from its individual labels, in order from the root to the leaf.
    ///
    /// - Parameter arrayLiteral: The labels making up the path, e.g. `["Top", "Science", "Astronomy"]`.
    public init(arrayLiteral elements: String...) {
        self.labels = elements
    }
}

extension PostgresLTree: CustomStringConvertible, LosslessStringConvertible {
    /// Create an `ltree` by splitting a dotted path into its labels.
    ///
    /// Empty labels, like in `a..b`, are dropped. Other invalid labels are left for Postgres to reject.
    ///
    /// - Parameter description: A dotted path, e.g. `Top.Science.Astronomy`.
    public init(_ description: String) {
        self.labels = description.split(separator: ".").map(String.init)
    }

    /// The path as a dotted string, e.g. `Top.Science.Astronomy`.
    public var description: String {
        self.labels.joined(separator: ".")
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
        for (index, label) in self.labels.enumerated() {
            if index != 0 {
                byteBuffer.writeInteger(UInt8(ascii: "."))
            }
            byteBuffer.writeString(label)
        }
    }
}
