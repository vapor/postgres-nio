extension PostgresData {
    public init(string: String) {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf8.count)
        buffer.writeString(string)
        self.init(type: .text, formatCode: .binary, value: buffer)
    }
    
    public var string: String? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
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
                #warning("TODO: fix force unwrap")
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
                    #warning("TODO: fix force unwrap")
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
            default: fatalError("Cannot decode String from \(self)")
            }
        case .text:
            guard let string = value.readString(length: value.readableBytes) else {
                fatalError()
            }
            return string
        }
    }
}
