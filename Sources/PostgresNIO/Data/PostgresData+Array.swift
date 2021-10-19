import NIOCore

extension PostgresData {
    public init<T>(array: [T])
        where T: PostgresDataConvertible
    {
        self.init(
            array: array.map { $0.postgresData },
            elementType: T.postgresDataType
        )
    }
    public init(array: [PostgresData?], elementType: PostgresDataType) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        // 0 if empty, 1 if not
        buffer.writeInteger(array.isEmpty ? 0 : 1, as: UInt32.self)
        // b
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
                    buffer.writeInteger(0, as: UInt32.self)
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

    public func array<T>(of type: T.Type = T.self) -> [T]?
        where T: PostgresDataConvertible
    {
        guard let array = self.array else {
            return nil
        }
        var items: [T] = []
        for data in array {
            guard let item = T(postgresData: data) else {
                // if we fail to convert any data, fail the entire array
                return nil
            }
            items.append(item)
        }
        return items
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
        guard let b = value.readInteger(as: UInt32.self) else {
            return nil
        }
        assert(b == 0, "Array b field did not equal zero")
        guard let type = value.readRawRepresentableInteger(as: PostgresDataType.self) else {
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
            let itemLength = value.readInteger(as: UInt32.self),
            let itemValue = value.readSlice(length: numericCast(itemLength))
        {
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

extension Array: PostgresDataConvertible where Element: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        guard let arrayType = Element.postgresDataType.arrayType else {
            fatalError("No array type for \(Element.postgresDataType)")
        }
        return arrayType
    }

    public init?(postgresData: PostgresData) {
        guard let array = postgresData.array(of: Element.self) else {
            return nil
        }
        self = array
    }

    public var postgresData: PostgresData? {
        return PostgresData(array: self)
    }
}
