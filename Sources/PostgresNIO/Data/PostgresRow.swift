public struct PostgresRow: CustomStringConvertible {
    final class LookupTable {
        let rowDescription: PostgresMessage.RowDescription
        let resultFormat: [PostgresFormatCode]

        struct Value {
            let index: Int
            let field: PostgresMessage.RowDescription.Field
        }
        
        private var _storage: [String: Value]?
        var storage: [String: Value] {
            if let existing = self._storage {
                return existing
            } else {
                let all = self.rowDescription.fields.enumerated().map { (index, field) in
                    return (field.name, Value(index: index, field: field))
                }
                let storage = [String: Value](all) { a, b in
                    // take the first value
                    return a
                }
                self._storage = storage
                return storage
            }
        }

        init(
            rowDescription: PostgresMessage.RowDescription,
            resultFormat: [PostgresFormatCode]
        ) {
            self.rowDescription = rowDescription
            self.resultFormat = resultFormat
        }

        func lookup(column: String) -> Value? {
            if let value = self.storage[column] {
                return value
            } else {
                return nil
            }
        }
    }

    internal let dataRow: PostgresMessage.DataRow
    internal let lookupTable: LookupTable

    public func column(_ column: String) -> PostgresData? {
        guard let entry = self.lookupTable.lookup(column: column) else {
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
            value: self.dataRow.columns[entry.index].value
        )
    }

    public var description: String {
        var row: [String: PostgresData] = [:]
        for field in self.lookupTable.rowDescription.fields {
            row[field.name] = self.column(field.name)
        }
        return row.description
    }
}
