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

extension PostgresRow: CustomStringConvertible {
    public var description: String {
        var row: [String: PostgresData] = [:]
        for field in self.rowDescription.fields {
            row[field.name] = self.column(field.name)
        }
        return row.description
    }
}
