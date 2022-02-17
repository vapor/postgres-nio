/// `PostgresRow` represents a single row that was received from the server for a query or prepared statement.
public struct PostgresRow {
    let lookupTable: [String: Int]
    let data: DataRow

    let columns: [RowDescription.Column]

    init(data: DataRow, lookupTable: [String: Int], columns: [RowDescription.Column]) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
    }

    // MARK: Pre async await interface

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

extension PostgresRow: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.data == rhs.data && lhs.columns == rhs.columns
    }
}

extension PostgresRow {
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column name to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable>(column: String, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        guard let index = self.lookupTable[column] else {
            preconditionFailure("A column '\(column)' does not exist.")
        }

        return try self.decode(column: index, as: type, file: file, line: line)
    }

    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column index to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable>(column index: Int, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        precondition(index < self.data.columnCount)

        let column = self.columns[index]
        let context = PSQLDecodingContext(
            jsonDecoder: self.jsonDecoder,
            columnName: column.name,
            columnIndex: index,
            file: file,
            line: line)

        guard var cellSlice = self.data[column: index] else {
            throw PSQLCastingError.missingData(targetType: T.self, type: column.dataType, context: context)
        }

        return try T.decode(from: &cellSlice, type: column.dataType, format: column.format, context: context)
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
