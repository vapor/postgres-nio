extension PostgresRowSequence {
    /// A ``PostgresColumns`` collection containing metadata about the columns in the query result.
    public var columns: PostgresColumns {
        PostgresColumns(underlying: self._columns)
    }
}

/// A collection of ``PostgresColumn`` column metadata for a PostgreSQL query result.
///
/// You can access metadata about the columns in a query result from ``PostgresRowSequence/columns``.
public struct PostgresColumns: Sequence, Sendable {
    public typealias Element = PostgresColumn

    var underlying: [RowDescription.Column]

    public func makeIterator() -> Iterator {
        Iterator(underlying: self.underlying.makeIterator())
    }

    public struct Iterator: IteratorProtocol {
        var underlying: [RowDescription.Column].Iterator

        public mutating func next() -> PostgresColumn? {
            guard let next = self.underlying.next() else {
                return nil
            }
            return PostgresColumn(underlying: next)
        }
    }
}

extension PostgresColumns: Collection, Equatable {
    public typealias Index = Int

    public var startIndex: Index { self.underlying.startIndex }
    public var endIndex: Index { self.underlying.endIndex }

    public subscript(position: Index) -> PostgresColumn {
        PostgresColumn(underlying: self.underlying[position])
    }

    public func index(after i: Int) -> Int {
        self.underlying.index(after: i)
    }
}

/// Metadata for a single column in a PostgreSQL query result.
public struct PostgresColumn: Hashable, Sendable {
    let underlying: RowDescription.Column

    /// The field name.
    public var name: String {
        self.underlying.name
    }

    /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    public var tableOID: Int32 {
        self.underlying.tableOID
    }

    /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    public var columnAttributeNumber: Int16 {
        self.underlying.columnAttributeNumber
    }

    /// The object ID of the field's data type.
    public var dataType: PostgresDataType {
        self.underlying.dataType
    }

    /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    public var dataTypeSize: Int16 {
        self.underlying.dataTypeSize
    }

    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    public var dataTypeModifier: Int32 {
        self.underlying.dataTypeModifier
    }

    /// The format being used for the field. Currently will be text or binary.
    /// In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be text.
    public var format: PostgresFormat {
        self.underlying.format
    }
}
