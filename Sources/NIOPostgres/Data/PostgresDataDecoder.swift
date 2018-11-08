#warning("keep this decoder internal, and publicize a different one")
public struct PostgresDataDecoder: Decoder {
    public var codingPath: [CodingKey] {
        return []
    }
    
    public var userInfo: [CodingUserInfoKey : Any] {
        return [:]
    }
    
    public let data: PostgresData
    
    public func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        return try SingleValueDecoder(decoder: self).decode(T.self)
    }
    
    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        #warning("Implement JSON decoding support")
        fatalError("Decoding structs from a PostgresData is not yet supported")
    }
    
    public func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        #warning("Implement array decoding support")
        fatalError("Decoding arrays from a PostgresData is not yet supported")
    }
    
    public func singleValueContainer() throws -> SingleValueDecodingContainer {
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
                case .numeric:
                    /// Represents the meta information preceeding a numeric value.
                    struct PostgreSQLNumericMetadata {
                        /// The number of digits after this metadata
                        var ndigits: Int16
                        /// How many of the digits are before the decimal point (always add 1)
                        var weight: Int16
                        /// If 0x4000, this number is negative. See NUMERIC_NEG in
                        /// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c
                        var sign: Int16
                        /// The number of sig digits after the decimal place (get rid of trailing 0s)
                        var dscale: Int16
                    }
                    
                    /// grab the numeric metadata from the beginning of the array
                    #warning("handle ! better")
                    let metadata = PostgreSQLNumericMetadata(
                        ndigits: value.readInteger()!,
                        weight: value.readInteger()!,
                        sign: value.readInteger()!,
                        dscale: value.readInteger()!
                    )
                    
                    guard metadata.ndigits > 0 else {
                        return "0"
                    }
                    
                    var integer = ""
                    var fractional = ""
                    for offset in 0..<metadata.ndigits {
                        /// extract current char and advance memory
                        #warning("handle ! better")
                        let char = value.readInteger(as: Int16.self)!
                        
                        /// convert the current char to its string form
                        let string: String
                        if char == 0 {
                            /// 0 means 4 zeros
                            string = "0000"
                        } else {
                            string = char.description
                        }
                        
                        /// depending on our offset, append the string to before or after the decimal point
                        if offset < metadata.weight + 1 {
                            // insert zeros (skip leading)
                            if offset > 0 {
                                integer += String(repeating: "0", count: 4 - string.count)
                            }
                            integer += string
                        } else {
                            // leading zeros matter with fractional
                            fractional += String(repeating: "0", count: 4 - string.count) + string
                        }
                    }
                    
                    if integer.count == 0 {
                        integer = "0"
                    }
                    
                    if fractional.count > metadata.dscale {
                        /// use the dscale to remove extraneous zeroes at the end of the fractional part
                        let lastSignificantIndex = fractional.index(
                            fractional.startIndex, offsetBy: Int(metadata.dscale)
                        )
                        fractional = String(fractional[..<lastSignificantIndex])
                    }
                    
                    /// determine whether fraction is empty and dynamically add `.`
                    let numeric: String
                    if fractional != "" {
                        numeric = integer + "." + fractional
                    } else {
                        numeric = integer
                    }
                    
                    /// use sign to determine adding a leading `-`
                    if (metadata.sign & 0x4000) != 0 {
                        return "-" + numeric
                    } else {
                        return numeric
                    }
                case .uuid: return value.readUUID()!.uuidString
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
            return try self.decode(float: Double.self)
        }
        
        func decode(_ type: Float.Type) throws -> Float {
            return try self.decode(float: Float.self)
        }
        
        func decode<F>(float: F.Type) throws -> F where F: BinaryFloatingPoint & LosslessStringConvertible {
            guard var value = self.decoder.data.value else {
                fatalError()
            }
            
            switch self.decoder.data.formatCode {
            case .binary:
                switch self.decoder.data.type {
                case .float4:
                    let float = value.readFloat(as: Float.self)!
                    return F(float)
                case .float8:
                    let double = value.readFloat(as: Double.self)!
                    return F(double)
                default: fatalError("Cannot decode \(F.self) from \(self.decoder.data)")
                }
            case .text:
                let string = try decode(String.self)
                #warning("better handle !")
                return F(string)!
            }
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
            guard var value = self.decoder.data.value else {
                fatalError()
            }
            
            switch self.decoder.data.formatCode {
            case .binary:
                #warning("Account for bit-size mismatch")
                switch self.decoder.data.type {
                case .int2:
                    assert(value.readableBytes == 2)
                    guard let int16 = value.readInteger(as: Int16.self) else {
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
            if let decodable = T.self as? CustomPostgresDecodable.Type {
                return try decodable.decode(from: self.decoder) as! T
            } else {
                return try T.init(from: self.decoder)
            }
        }
    }
}
