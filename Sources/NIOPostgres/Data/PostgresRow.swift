public struct PostgresRow: CustomStringConvertible {
    let dataRow: PostgresMessage.DataRow
    
    final class LookupTable {
        struct FieldKey: Hashable {
            var columnName: String
            var tableOID: UInt32
        }
        
        struct FieldValue {
            var field: PostgresMessage.RowDescription.Field
            var offset: Int
        }
        
        let rowDescription: PostgresMessage.RowDescription
        let tableNames: PostgresConnection.TableNames?
        
        private var isInitialized: Bool
        private var map: [FieldKey: FieldValue]
        
        init(
            rowDescription: PostgresMessage.RowDescription,
            tableNames: PostgresConnection.TableNames?
        ) {
            self.rowDescription = rowDescription
            self.isInitialized = false
            self.map = [:]
            self.tableNames = tableNames
        }
        
        func initialize(with rowDescription: PostgresMessage.RowDescription) {
            for (i, field) in rowDescription.fields.enumerated() {
                let fieldID = FieldKey(columnName: field.name, tableOID: field.tableOID)
                map[fieldID] = FieldValue(field: field, offset: i)
            }
        }
    
        func tableOID(name: String?) -> UInt32 {
            guard let name = name else {
                return 0
            }
            
            guard let tableNames = self.tableNames else {
                fatalError("Table names have not been loaded")
            }
            
            guard let tableOID = tableNames.oid(forName: name) else {
                #warning("consider returning 0 here")
                fatalError("Unknown table name: \(name)")
            }
            
            return tableOID
        }
        
        func lookup(column: String, tableOID: UInt32) -> FieldValue? {
            if !isInitialized {
                self.initialize(with: rowDescription)
                self.isInitialized = true
            }
            let key = FieldKey(columnName: column, tableOID: tableOID)
            return map[key]
        }
    }
    
    let lookupTable: LookupTable
    
    public func decode<T>(_ decodable: T.Type, table: String? = nil) throws -> T
        where T: Decodable
    {
        return try decode(T.self, tableOID: lookupTable.tableOID(name: table))
    }
    
    public func decode<T>(_ decodable: T.Type, at column: String, table: String? = nil) throws -> T?
        where T: Decodable
    {
        return try decode(T.self, at: column, tableOID: lookupTable.tableOID(name: table))
    }
    
    public func decode<T>(_ decodable: T.Type, tableOID: UInt32) throws -> T
        where T: Decodable
    {
        let decoder = PostgresRowDecoder(row: self, tableOID: tableOID)
        return try T(from: decoder)
    }
    
    public func decode<T>(_ decodable: T.Type, at column: String, tableOID: UInt32) throws -> T?
        where T: Decodable
    {
        guard let data = self.data(at: column, tableOID: tableOID) else {
            return nil
        }
        return try PostgresDataDecoder(data: data).decode(T.self)
    }
    
    public func data(at column: String, tableOID: UInt32 = 0) -> PostgresData? {
        guard let result = self.lookupTable.lookup(column: column, tableOID: tableOID) else {
            return nil
        }
        return PostgresData(
            type: result.field.dataType,
            typeModifier: result.field.dataTypeModifier,
            formatCode: result.field.formatCode,
            value: dataRow.columns[result.offset].value
        )
    }
    
    public var description: String {
        var row: [String: String] = [:]
        for (i, field) in lookupTable.rowDescription.fields.enumerated() {
            let column = dataRow.columns[i]
            let data: String
            if let value = column.value {
                switch field.formatCode {
                case .text: data = value.getString(at: value.readerIndex, length: value.readableBytes) ?? "<mal-encoded string>"
                case .binary: data = "0x" + value.readableBytesView.hexdigest()
                }
            } else {
                data = "<null>"
            }
            row[field.name] = data
        }
        return row.description
    }
}
