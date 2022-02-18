import NIOCore
import class Foundation.JSONDecoder

/// `PostgresRow` represents a single table row that is received from the server for a query or prepared statement.
/// Its element type is ``PostgresCell``.
///
/// Please note that the ``PostgresRow`` only implements the ``Swift/Collection`` protocol, but not the
/// ``Swift/RandomAccessCollection`` protocol. This means that access to a random cell is *O(n)* and not *O(1)*.
/// If you want to randomly access cells from the ``PostgresRow`` create a new ``PostgresRandomAccessRow``
/// and query it instead.
public struct PostgresRow {
    let lookupTable: [String: Int]
    let data: DataRow

    let columns: [RowDescription.Column]

    init(data: DataRow, lookupTable: [String: Int], columns: [RowDescription.Column]) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
    }
}

extension PostgresRow: Collection {
    public typealias Element = PostgresCell
    public typealias Index = Int

    public subscript(position: Int) -> PostgresCell {
        let column = self.columns[position]
        return PostgresCell(
            bytes: self.data[column: position],
            dataType: column.dataType,
            format: column.format,
            columnName: column.name,
            columnIndex: position
        )
    }

    public var startIndex: Int {
        0
    }

    public var endIndex: Int {
        self.data.count
    }

    public func index(after i: Int) -> Int {
        i + 1
    }

    public var count: Int {
        self.data.count
    }
}

extension PostgresRow: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.data == rhs.data && lhs.columns == rhs.columns
    }
}

/// A random access row of ``PostgresCell``s. Its initialization is *O(n)* where *n* is the number of columns
/// in the row. All subsequent cell access are *O(1)*.
struct PostgresRandomAccessRow {
    let columns: [RowDescription.Column]
    let cells: [ByteBuffer?]
    let lookupTable: [String: Int]

    init(_ row: PostgresRow) {
        self.cells = row.data.map { $0 }
        self.columns = row.columns
        self.lookupTable = row.lookupTable
    }
}

extension PostgresRandomAccessRow: RandomAccessCollection {
    typealias Element = PostgresCell
    typealias Index = Int

    var startIndex: Int {
        0
    }

    var endIndex: Int {
        self.columns.count
    }

    var count: Int {
        self.columns.count
    }

    func index(after index: Int) -> Int {
        guard index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }
        return index + 1
    }

    subscript(index: Int) -> PostgresCell {
        guard index < self.endIndex else {
            preconditionFailure("index out of bounds")
        }

        return PostgresCell(
            bytes: self.cells[index],
            dataType: self.columns[index].dataType,
            format: self.columns[index].format,
            columnName: self.columns[index].name,
            columnIndex: index
        )
    }
}

// MARK: Deprecated API

extension PostgresRow {
    public var rowDescription: PostgresMessage.RowDescription {
        let fields = self.columns.map { column in
            PostgresMessage.RowDescription.Field(
                name: column.name,
                tableOID: UInt32(column.tableOID),
                columnAttributeNumber: column.columnAttributeNumber,
                dataType: PostgresDataType(UInt32(column.dataType.rawValue)),
                dataTypeSize: column.dataTypeSize,
                dataTypeModifier: column.dataTypeModifier,
                formatCode: .init(psqlFormatCode: column.format)
            )
        }
        return PostgresMessage.RowDescription(fields: fields)
    }

    public var dataRow: PostgresMessage.DataRow {
        let columns = self.data.map {
            PostgresMessage.DataRow.Column(value: $0)
        }
        return PostgresMessage.DataRow(columns: columns)
    }

    @available(*, deprecated, message: """
        This call is O(n) where n is the number of cells in the row. For random access to cells
        in a row create a PostgresRandomAccessCollection from the row first and use its subscript
        methods.
        """)
    public func column(_ column: String) -> PostgresData? {
        guard let index = self.lookupTable[column] else {
            return nil
        }

        return PostgresData(
            type: self.columns[index].dataType,
            typeModifier: self.columns[index].dataTypeModifier,
            formatCode: .binary,
            value: self.data[column: index]
        )
    }
}

extension PostgresRow: CustomStringConvertible {
    public var description: String {
        var row: [String: PostgresData] = [:]
        for field in self.rowDescription.fields {
            row[field.name] = self.column(field.name)
        }
        return row.description
    }
}

// MARK: Fill in API (remove as soon as we have real decode methods)

extension PostgresRandomAccessRow {
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column name to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable, JSONDecoder: PostgresJSONDecoder>(
        column: String,
        as type: T.Type,
        context: PostgresDecodingContext<JSONDecoder>,
        file: String = #file, line: Int = #line
    ) throws -> T {
        guard let index = self.lookupTable[column] else {
            preconditionFailure("A column '\(column)' does not exist.")
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
    func decode<T: PSQLDecodable, JSONDecoder: PostgresJSONDecoder>(
        column index: Int,
        as type: T.Type,
        context: PostgresDecodingContext<JSONDecoder>,
        file: String = #file, line: Int = #line
    ) throws -> T {
        precondition(index < self.columns.count)

        let column = self.columns[index]

        var cellSlice = self.cells[index]
        do {
            return try T.decodeRaw(from: &cellSlice, type: column.dataType, format: column.format, context: context)
        } catch let code as PostgresCastingError.Code {
            throw PostgresCastingError(
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

extension PostgresRandomAccessRow {
    // TODO: Remove this function. Only here to keep the tests running as of today.
    func decode<T: PSQLDecodable>(column: String, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        let context = PostgresDecodingContext(jsonDecoder: Foundation.JSONDecoder())
        return try self.decode(column: column, as: type, context: context, file: file, line: line)
    }

    // TODO: Remove this function. Only here to keep the tests running as of today.
    func decode<T: PSQLDecodable>(column index: Int, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        let context = PostgresDecodingContext(jsonDecoder: Foundation.JSONDecoder())
        return try self.decode(column: index, as: type, context: context, file: file, line: line)
    }
}
