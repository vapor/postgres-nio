import NIOCore

extension PostgresData {
    public init(array: [PostgresData?], elementType: PostgresDataType) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        // 0 if empty, 1 if not
        buffer.writeInteger(array.isEmpty ? 0 : 1, as: UInt32.self)
        // b - this gets ignored by psql
        buffer.writeInteger(0, as: UInt32.self)
        // array element type
        buffer.writeInteger(elementType.rawValue)

        // continue if the array is not empty
        if !array.isEmpty {
            // length of array
            buffer.writeInteger(numericCast(array.count), as: UInt32.self)
            // dimensions
            buffer.writeInteger(1, as: UInt32.self)

            for item in array {
                if let item = item, var value = item.value {
                    buffer.writeInteger(numericCast(value.readableBytes), as: UInt32.self)
                    buffer.writeBuffer(&value)
                } else {
                    buffer.writeInteger(-1, as: Int32.self)
                }
            }
        }

        guard let arrayType = elementType.arrayType else {
            fatalError("No array type for \(elementType)")
        }
        self.init(
            type: arrayType,
            typeModifier: nil,
            formatCode: .binary,
            value: buffer
        )
    }

    public var array: [PostgresData]? {
        guard case .binary = self.formatCode else {
            return nil
        }
        guard var value = self.value else {
            return nil
        }
        // ensures the data type is actually an array
        guard self.type.elementType != nil else {
            return nil
        }
        guard let isNotEmpty = value.readInteger(as: UInt32.self) else {
            return nil
        }
        // b
        guard let _ = value.readInteger(as: UInt32.self) else {
            return nil
        }
        guard let type = value.readInteger(as: PostgresDataType.self) else {
            return nil
        }
        guard isNotEmpty == 1 else {
            return []
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
            let itemLength = value.readInteger(as: Int32.self)
        {
            let itemValue = itemLength == -1 ? nil : value.readSlice(length: numericCast(itemLength))
            let data = PostgresData(
                type: type,
                typeModifier: nil,
                formatCode: self.formatCode,
                value: itemValue
            )
            array.append(data)
        }
        return array
    }
}
