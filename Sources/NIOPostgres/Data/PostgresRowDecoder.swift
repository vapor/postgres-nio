struct PostgresRowDecoder: Decoder {
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
        fatalError("Decoding arrays from a PostgresRow is not supported")
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        fatalError("Decoding single values from a PostgresRow is not supported")
    }
    
    struct KeyedDecoder<Key>: KeyedDecodingContainerProtocol where Key: CodingKey {
        var codingPath: [CodingKey] {
            return []
        }
        
        var allKeys: [Key] {
            #warning("Implement all keys if necessary")
            fatalError("KeyedDecoder.allKeys is not supported")
        }
        
        let decoder: PostgresRowDecoder
        
        func contains(_ key: Key) -> Bool {
            return decoder.row.lookupTable.lookup(column: key.stringValue, tableOID: decoder.tableOID) != nil
        }
        
        func decodeNil(forKey key: Key) throws -> Bool {
            guard let data = decoder.row.data(at: key.stringValue, tableOID: decoder.tableOID) else {
                return true
            }
            return data.value == nil
        }
        
        func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
            guard let data = decoder.row.data(at: key.stringValue, tableOID: decoder.tableOID) else {
                fatalError("No value at key \(key)")
            }
            return try PostgresDataDecoder(data: data).decode(T.self)
        }
        
        func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
            fatalError("Decoding nested containers from a PostgresRow is not supported")
        }
        
        func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
            fatalError("Decoding nested containers from a PostgresRow is not supported")
        }
        
        func superDecoder() throws -> Decoder {
            return decoder
        }
        
        func superDecoder(forKey key: Key) throws -> Decoder {
            return decoder
        }
    }
}
