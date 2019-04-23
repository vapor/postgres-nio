public struct PostgresNumeric: CustomStringConvertible, CustomDebugStringConvertible {
    /// The number of digits after this metadata
    var ndigits: Int16
    /// How many of the digits are before the decimal point (always add 1)
    var weight: Int16
    /// If 0x4000, this number is negative. See NUMERIC_NEG in
    /// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c
    var sign: Int16
    /// The number of sig digits after the decimal place (get rid of trailing 0s)
    var dscale: Int16

    var value: ByteBuffer

    public var description: String {
        return self.string
    }

    public var debugDescription: String {
        return """
        ndigits: \(self.ndigits)
        weight: \(self.weight)
        sign: \(self.sign)
        dscale: \(self.dscale)
        value: \(self.value.debugDescription)
        """
    }

    public var double: Double? {
        return Double(self.string)
    }

    public var string: String {
        guard self.ndigits > 0 else {
            return "0"
        }

        var integer = ""
        var fractional = ""

        var value = self.value
        for offset in 0..<self.ndigits {
            /// extract current char and advance memory
            let char = value.readInteger(endianness: .big, as: Int16.self) ?? 0

            /// convert the current char to its string form
            let string: String
            if char == 0 {
                /// 0 means 4 zeros
                string = "0000"
            } else {
                string = char.description
            }

            /// depending on our offset, append the string to before or after the decimal point
            if offset < self.weight + 1 {
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

        if fractional.count > self.dscale {
            /// use the dscale to remove extraneous zeroes at the end of the fractional part
            let lastSignificantIndex = fractional.index(fractional.startIndex, offsetBy: Int(self.dscale))
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
        if (self.sign & 0x4000) != 0 {
            return "-" + numeric
        } else {
            return numeric
        }
    }

    init?(buffer: inout ByteBuffer) {
        guard let ndigits = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.ndigits = ndigits
        guard let weight = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.weight = weight
        guard let sign = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.sign = sign
        guard let dscale = buffer.readInteger(endianness: .big, as: Int16.self) else {
            return nil
        }
        self.dscale = dscale
        self.value = buffer
    }
}

extension PostgresData {
    public var numeric: PostgresNumeric? {
        /// create mutable value since we will be using `.extract` which advances the buffer's view
        guard var value = self.value else {
            return nil
        }

        /// grab the numeric metadata from the beginning of the array
        guard let metadata = PostgresNumeric(buffer: &value) else {
            return nil
        }

        return metadata
    }
}
