public struct PostgresRow: CustomStringConvertible {
    final class LookupTable {
        let rowDescription: PostgresMessage.RowDescription
        let resultFormat: [PostgresFormatCode]
        
        init(
            rowDescription: PostgresMessage.RowDescription,
            resultFormat: [PostgresFormatCode]
        ) {
            self.rowDescription = rowDescription
            self.resultFormat = resultFormat
        }
        
        func lookup(column: String, tableOID: UInt32) -> (Int, PostgresMessage.RowDescription.Field)? {
            for (i, field) in self.rowDescription.fields.enumerated() {
                if (tableOID == 0 || field.tableOID == tableOID) && field.name == column {
                    return (i, field)
                }
            }
            return nil
        }
    }
    
    let dataRow: PostgresMessage.DataRow
    let lookupTable: LookupTable
    
    public func column(_ column: String, tableOID: UInt32 = 0) -> PostgresData? {
        guard let (offset, field) = self.lookupTable.lookup(column: column, tableOID: tableOID) else {
            return nil
        }
        let formatCode: PostgresFormatCode
        switch self.lookupTable.resultFormat.count {
        case 1: formatCode = self.lookupTable.resultFormat[0]
        default: formatCode = field.formatCode
        }
        return PostgresData(
            type: field.dataType,
            typeModifier: field.dataTypeModifier,
            formatCode: formatCode,
            value: self.dataRow.columns[offset].value
        )
    }
    
    public var description: String {
        var row: [String: PostgresData] = [:]
        for field in self.lookupTable.rowDescription.fields {
            #warning("reverse lookup table names for desc")
            row[field.name] = self.column(field.name, tableOID: field.tableOID)
        }
        return row.description
    }
}
