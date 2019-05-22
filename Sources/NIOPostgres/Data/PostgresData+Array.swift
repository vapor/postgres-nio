extension PostgresData {
    public var array: [PostgresData]? {
        guard var value = self.value else {
            return nil
        }
        // ensures the data type is actually an array
        guard self.type.elementType != nil else {
            return nil
        }

        guard let isNotNull = value.readInteger(as: UInt32.self) else {
            return nil
        }
        guard isNotNull == 1 else {
            return nil
        }
        guard let b = value.readInteger(as: UInt32.self) else {
            return nil
        }
        assert(b == 0, "Array b field did not equal zero")
        guard let type = value.readInteger(as: PostgresDataType.self) else {
            return nil
        }
        guard let length = value.readInteger(as: UInt32.self) else {
            return nil
        }
        assert(length >= 0, "Invalid length")

        guard let dimensions = value.readInteger(as: UInt32.self) else {
            return nil
        }
        assert(dimensions == 1, "Multi-dimensional arrays not yet supported")

        var array: [PostgresData] = []
        while
            let itemLength = value.readInteger(as: UInt32.self),
            let itemValue = value.readSlice(length: numericCast(itemLength))
        {
            let data = PostgresData(type: type, typeModifier: nil, formatCode: self.formatCode, value: itemValue)
            array.append(data)
        }
        return array
    }
}
