import NIOCore
import class Foundation.JSONDecoder

/// `PostgresRow` represents a single table row that is received from the server for a query or a prepared statement.
/// Its element type is ``PostgresCell``.
///
/// - Warning: Please note that random access to cells in a ``PostgresRow`` have O(n) time complexity. If you require
///            random access to cells in O(1) create a new ``PostgresRandomAccessRow`` with the given row and
///            access it instead.
public struct PostgresRow: Sendable {
    @usableFromInline
    let lookupTable: [String: Int]
    @usableFromInline
    let data: DataRow
    @usableFromInline
    let columns: [RowDescription.Column]

    init(data: DataRow, lookupTable: [String: Int], columns: [RowDescription.Column]) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
    }
}

extension PostgresRow: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        // we don't need to compare the lookup table here, as the looup table is only derived
        // from the column description.
        lhs.data == rhs.data && lhs.columns == rhs.columns
    }
}

extension PostgresRow: Sequence {
    public typealias Element = PostgresCell

    public struct Iterator: IteratorProtocol {
        public typealias Element = PostgresCell

        private(set) var columnIndex: Array<RowDescription.Column>.Index
        private(set) var columnIterator: Array<RowDescription.Column>.Iterator
        private(set) var dataIterator: DataRow.Iterator

        init(_ row: PostgresRow) {
            self.columnIndex = 0
            self.columnIterator = row.columns.makeIterator()
            self.dataIterator = row.data.makeIterator()
        }

        public mutating func next() -> PostgresCell? {
            guard let bytes = self.dataIterator.next() else {
                return nil
            }

            let column = self.columnIterator.next()!

            defer { self.columnIndex += 1 }

            return PostgresCell(
                bytes: bytes,
                dataType: column.dataType,
                format: column.format,
                columnName: column.name,
                columnIndex: columnIndex
            )
        }
    }

    public func makeIterator() -> Iterator {
        Iterator(self)
    }
}

extension PostgresRow: Collection {
    public struct Index: Comparable {
        var cellIndex: DataRow.Index
        var columnIndex: Array<RowDescription.Column>.Index

        // Only needed implementation for comparable. The compiler synthesizes the rest from this.
        public static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.columnIndex < rhs.columnIndex
        }
    }

    public subscript(position: Index) -> PostgresCell {
        let column = self.columns[position.columnIndex]
        return PostgresCell(
            bytes: self.data[position.cellIndex],
            dataType: column.dataType,
            format: column.format,
            columnName: column.name,
            columnIndex: position.columnIndex
        )
    }

    public var startIndex: Index {
        Index(
            cellIndex: self.data.startIndex,
            columnIndex: 0
        )
    }

    public var endIndex: Index {
        Index(
            cellIndex: self.data.endIndex,
            columnIndex: self.columns.count
        )
    }

    public func index(after i: Index) -> Index {
        Index(
            cellIndex: self.data.index(after: i.cellIndex),
            columnIndex: self.columns.index(after: i.columnIndex)
        )
    }

    public var count: Int {
        self.data.count
    }
}

extension PostgresRow {
    public func makeRandomAccess() -> PostgresRandomAccessRow {
        PostgresRandomAccessRow(self)
    }
}

/// A random access row of ``PostgresCell``s. Its initialization is O(n) where n is the number of columns
/// in the row. All subsequent cell access are O(1).
public struct PostgresRandomAccessRow {
    let columns: [RowDescription.Column]
    let cells: [ByteBuffer?]
    let lookupTable: [String: Int]

    public init(_ row: PostgresRow) {
        self.cells = [ByteBuffer?](row.data)
        self.columns = row.columns
        self.lookupTable = row.lookupTable
    }
}

extension PostgresRandomAccessRow: Sendable, RandomAccessCollection {
    public typealias Element = PostgresCell
    public typealias Index = Int

    public var startIndex: Int {
        0
    }

    public var endIndex: Int {
        self.columns.count
    }

    public var count: Int {
        self.columns.count
    }

    public subscript(index: Int) -> PostgresCell {
        guard index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }
        let column = self.columns[index]
        return PostgresCell(
            bytes: self.cells[index],
            dataType: column.dataType,
            format: column.format,
            columnName: column.name,
            columnIndex: index
        )
    }

    public subscript(name: String) -> PostgresCell {
        guard let index = self.lookupTable[name] else {
            fatalError(#"A column "\#(name)" does not exist."#)
        }
        return self[index]
    }

    /// Checks if the row contains a cell for the given column name.
    /// - Parameter column: The column name to check against
    /// - Returns: `true` if the row contains this column, `false` if it does not.
    public func contains(_ column: String) -> Bool {
        self.lookupTable[column] != nil
    }
}

extension PostgresRandomAccessRow {
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column name to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(
        column: String,
        as type: T.Type,
        context: PostgresDecodingContext<JSONDecoder>,
        file: String = #fileID, line: Int = #line
    ) throws -> T {
        guard let index = self.lookupTable[column] else {
            fatalError(#"A column "\#(column)" does not exist."#)
        }

        return try self.decode(column: index, as: type, context: context, file: file, line: line)
    }

    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column index to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(
        column index: Int,
        as type: T.Type,
        context: PostgresDecodingContext<JSONDecoder>,
        file: String = #fileID, line: Int = #line
    ) throws -> T {
        precondition(index < self.columns.count)

        let column = self.columns[index]

        var cellSlice = self.cells[index]
        do {
            return try T._decodeRaw(from: &cellSlice, type: column.dataType, format: column.format, context: context)
        } catch let code as PostgresDecodingError.Code {
            throw PostgresDecodingError(
                code: code,
                columnName: self.columns[index].name,
                columnIndex: index,
                targetType: T.self,
                postgresType: self.columns[index].dataType,
                postgresFormat: self.columns[index].format,
                postgresData: cellSlice,
                file: file,
                line: line
            )
        }
    }
}
