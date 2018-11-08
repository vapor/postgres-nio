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
        
        func decode(_ type: Bool.Type) throws -> Bool {
            fatalError()
        }
        
        func decode(_ type: String.Type) throws -> String {
            guard var value = self.decoder.data.value else {
                fatalError()
            }
            
            switch self.decoder.data.formatCode {
            case .binary:
                switch self.decoder.data.type {
                case .varchar, .text:
                    guard let string = value.readString(length: value.readableBytes) else {
                        fatalError()
                    }
                    return string
                default: fatalError("Cannot decode String from \(self.decoder.data)")
                }
            case .text:
                guard let string = value.readString(length: value.readableBytes) else {
                    fatalError()
                }
                return string
            }
        }
        
        func decode(_ type: Double.Type) throws -> Double {
            fatalError()
        }
        
        func decode(_ type: Float.Type) throws -> Float {
            fatalError()
        }
        
        func decode(_ type: Int.Type) throws -> Int {
            return try self.decode(integer: Int.self)
        }
        
        func decode(_ type: Int8.Type) throws -> Int8 {
            return try self.decode(integer: Int8.self)
        }
        
        func decode(_ type: Int16.Type) throws -> Int16 {
            return try self.decode(integer: Int16.self)
        }
        
        func decode(_ type: Int32.Type) throws -> Int32 {
            return try self.decode(integer: Int32.self)
        }
        
        func decode(_ type: Int64.Type) throws -> Int64 {
            return try self.decode(integer: Int64.self)
        }
        
        func decode(_ type: UInt.Type) throws -> UInt {
            return try self.decode(integer: UInt.self)
        }
        
        func decode(_ type: UInt8.Type) throws -> UInt8 {
            return try self.decode(integer: UInt8.self)
        }
        
        func decode(_ type: UInt16.Type) throws -> UInt16 {
            return try self.decode(integer: UInt16.self)
        }
        
        func decode(_ type: UInt32.Type) throws -> UInt32 {
            return try self.decode(integer: UInt32.self)
        }
        
        func decode(_ type: UInt64.Type) throws -> UInt64 {
            return try self.decode(integer: UInt64.self)
        }
        
        func decode<I>(integer: I.Type) throws -> I where I: FixedWidthInteger {
            guard let value = self.decoder.data.value else {
                fatalError()
            }
            
            switch self.decoder.data.formatCode {
            case .binary:
                #warning("Account for bit-size mismatch")
                switch self.decoder.data.type {
                case .int2:
                    assert(value.readableBytes == 2)
                    guard let int16 = value.getInteger(at: value.readerIndex, as: Int16.self) else {
                        fatalError()
                    }
                    return I(int16)
                case .int4, .regproc:
                    assert(value.readableBytes == 4)
                    guard let int32 = value.getInteger(at: value.readerIndex, as: Int32.self) else {
                        fatalError()
                    }
                    return I(int32)
                case .oid:
                    assert(value.readableBytes == 4)
                    guard let uint32 = value.getInteger(at: value.readerIndex, as: UInt32.self) else {
                        fatalError()
                    }
                    return I(uint32)
                case .int8:
                    assert(value.readableBytes == 8)
                    guard let int64 = value.getInteger(at: value.readerIndex, as: Int64.self) else {
                        fatalError()
                    }
                    return I(int64)
                case .numeric:
                    #warning("Use numeric converter")
                    fatalError("use numeric converter")
                default: fatalError("Cannot decode \(I.self) from \(self.decoder.data)")
                }
            case .text:
                let string = try self.decode(String.self)
                guard let converted = I(string) else {
                    fatalError()
                }
                return converted
            }
        }
        
        func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
            if let decodable = T.self as? PostgresDataConvertible.Type {
                return decodable.init(postgresData: decoder.data)! as! T
            } else {
                return try T.init(from: self.decoder)
            }
        }
    }
}
