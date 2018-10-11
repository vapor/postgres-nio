public struct PostgresRow: CustomStringConvertible {
    let fields: [PostgresMessage.RowDescription.Field]
    let columns: [PostgresMessage.DataRow.Column]
    
    public func decode<T>(_ decodable: T.Type, at column: String, tableOID: UInt32 = 0) -> T?
        where T: PostgresDataDecodable
    {
        guard let data = self.data(at: column, tableOID: tableOID) else {
            return nil
        }
        return T.decode(from: data)
    }
    
    public func data(at column: String, tableOID: UInt32 = 0) -> PostgresData? {
        for (i, field) in fields.enumerated() {
            if field.name == column && (field.tableOID == 0 || tableOID == 0 || field.tableOID == tableOID) {
                return PostgresData(
                    type: field.dataType,
                    typeModifier: field.dataTypeModifier,
                    formatCode: field.formatCode,
                    value: columns[i].value
                )
            }
        }
        return nil
    }
    
    public var description: String {
        var row: [String: String] = [:]
        for (i, field) in fields.enumerated() {
            let column = columns[i]
            let data: String
            if let value = column.value {
                switch field.formatCode {
                case .text: data = String(bytes: value, encoding: .utf8) ?? "<mal-encoded string>"
                case .binary: data = "0x" + value.hexdigest()
                }
            } else {
                data = "<null>"
            }
            row[field.name] = data
        }
        return row.description
    }
}
