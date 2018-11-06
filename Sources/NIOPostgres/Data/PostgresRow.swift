public struct PostgresRow: CustomStringConvertible {
    let rowDescription: PostgresMessage.RowDescription
    let dataRow: PostgresMessage.DataRow
    
    public func decode<T>(_ decodable: T.Type, tableOID: UInt32 = 0) throws -> T
        where T: Decodable
    {
        let decoder = PostgresRowDecoder(row: self, tableOID: tableOID)
        return try T(from: decoder)
    }
    
    public func decode<T>(_ decodable: T.Type, at column: String, tableOID: UInt32 = 0) -> T?
        where T: PostgresDataConvertible
    {
        guard let data = self.data(at: column, tableOID: tableOID) else {
            return nil
        }
        return T(postgresData: data)
    }
    
    public func data(at column: String, tableOID: UInt32 = 0) -> PostgresData? {
        for (i, field) in rowDescription.fields.enumerated() {
            if field.name == column && (field.tableOID == 0 || tableOID == 0 || field.tableOID == tableOID) {
                return PostgresData(
                    type: field.dataType,
                    typeModifier: field.dataTypeModifier,
                    formatCode: field.formatCode,
                    value: dataRow.columns[i].value
                )
            }
        }
        return nil
    }
    
    public var description: String {
        #warning("look into optimizing this")
        var row: [String: String] = [:]
        for (i, field) in rowDescription.fields.enumerated() {
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


private struct PostgresRowDecoder: Decoder {
    var codingPath: [CodingKey] {
        return []
    }
    
    var userInfo: [CodingUserInfoKey : Any] {
        return [:]
    }
    
    let row: PostgresRow
    let tableOID: UInt32
    
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        return KeyedDecodingContainer(KeyedDecoder(decoder: self))
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        fatalError()
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        fatalError()
    }
    
    struct KeyedDecoder<Key>: KeyedDecodingContainerProtocol where Key: CodingKey {
        var codingPath: [CodingKey] {
            return []
        }
        
        var allKeys: [Key] {
            #warning("optimize all keys checking")
            fatalError()
        }
        
        let decoder: PostgresRowDecoder
        
        func contains(_ key: Key) -> Bool {
            #warning("optimize contains checking")
            return true
        }
        
        func decodeNil(forKey key: Key) throws -> Bool {
            #warning("optimize nil decode checking")
            return false
        }
        
        func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
            return decoder.row.decode(T.self, at: key.stringValue, tableOID: decoder.tableOID)
        }
        
        func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
            fatalError()
        }
        
        func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
            fatalError()
        }
        
        func superDecoder() throws -> Decoder {
            return decoder
        }
        
        func superDecoder(forKey key: Key) throws -> Decoder {
            return decoder
        }
    }
}
