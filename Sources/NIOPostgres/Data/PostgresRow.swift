public struct PostgresRow: CustomStringConvertible {
    final class LookupTable {
        let rowDescription: PostgresMessage.RowDescription
        let resultFormat: [PostgresFormatCode]

        struct Entry {
            let indexInRow: Int
            let field: PostgresMessage.RowDescription.Field
        }
        private var _columnNameToIndexLookupTable: [String: [Entry]]?
        var columnNameToIndexLookupTable: [String: [Entry]] {
            if let existing = _columnNameToIndexLookupTable {
                return existing
            }

            var columnNameToIndexLookupTable: [String: [Entry]] = [:]
            for (fieldIndex, field) in rowDescription.fields.enumerated() {
                columnNameToIndexLookupTable[field.name, default: []].append(.init(indexInRow: fieldIndex, field: field))
            }
            self._columnNameToIndexLookupTable = columnNameToIndexLookupTable
            return columnNameToIndexLookupTable
        }

        init(
            rowDescription: PostgresMessage.RowDescription,
            resultFormat: [PostgresFormatCode]
        ) {
            self.rowDescription = rowDescription
            self.resultFormat = resultFormat
        }

        func lookup(column: String, tableOID: UInt32) -> Entry? {
            guard let columnTable = columnNameToIndexLookupTable[column]
                else { return nil }

            if tableOID == 0 {
                return columnTable.first
            } else {
                return columnTable.first { $0.field.tableOID == tableOID }
            }
        }
    }

    let dataRow: PostgresMessage.DataRow
    let lookupTable: LookupTable

    public func column(_ column: String, tableOID: UInt32 = 0) -> PostgresData? {
        guard let entry = self.lookupTable.lookup(column: column, tableOID: tableOID) else {
            return nil
        }
        let formatCode: PostgresFormatCode
        switch self.lookupTable.resultFormat.count {
        case 1: formatCode = self.lookupTable.resultFormat[0]
        default: formatCode = entry.field.formatCode
        }
        return PostgresData(
            type: entry.field.dataType,
            typeModifier: entry.field.dataTypeModifier,
            formatCode: formatCode,
            value: self.dataRow.columns[entry.indexInRow].value
        )
    }

    public var description: String {
        var row: [String: PostgresData] = [:]
        for field in self.lookupTable.rowDescription.fields {
            #warning("TODO: reverse lookup table names for desc")
            row[field.name + "(\(field.tableOID))"] = self.column(field.name, tableOID: field.tableOID)
        }
        return row.description
    }
}
