struct PostgresDataDecoder: Decoder {
    var codingPath: [CodingKey] {
        return []
    }
    
    var userInfo: [CodingUserInfoKey : Any] {
        return [:]
    }
    
    let data: PostgresData
    
    func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        return try SingleValueDecoder(decoder: self).decode(T.self)
    }
    
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        #warning("Implement JSON decoding support")
        fatalError("Decoding structs from a PostgresData is not yet supported")
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        #warning("Implement array decoding support")
        fatalError("Decoding arrays from a PostgresData is not yet supported")
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        return SingleValueDecoder(decoder: self)
    }
    
    struct SingleValueDecoder: SingleValueDecodingContainer {
        var codingPath: [CodingKey] {
            return []
        }
        
        let decoder: PostgresDataDecoder
        
        func decodeNil() -> Bool {
            return decoder.data.value == nil
        }
        
        func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
            guard let decodable = T.self as? PostgresDataConvertible.Type else {
                fatalError("\(T.self) is not PostgresDataConvertible")
            }
            return decodable.init(postgresData: decoder.data)! as! T
        }
    }
}
