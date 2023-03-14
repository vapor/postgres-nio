extension PostgresData {
    public init(int8Range: Range<Int64>) {
        var buffer = ByteBufferAllocator().buffer(capacity: 25)
        buffer.writeInteger(2, as: Int8.self)
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(int8Range.lowerBound)
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(int8Range.upperBound)
        self.init(type: .int8Range, formatCode: .binary, value: buffer)
    }
    
    public var int8Range: Range<Int64>? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .int8Range:
                guard value.readInteger(as: Int8.self) == 2 else {
                    return nil
                }

                guard value.readInteger(as: Int32.self) == 8 else {
                    return nil
                }

                guard let lowerBound: Int64 = value.readInteger() else {
                    return nil
                }

                guard value.readInteger(as: Int32.self) == 8 else {
                    return nil
                }

                guard let upperBound: Int64 = value.readInteger() else {
                    return nil
                }
                
                return lowerBound..<upperBound
            default:
                return nil
            }
        case .text:
            return nil
        }
    }
}

extension Range: PostgresDataConvertible where Bound == Int64 {
    public static var postgresDataType: PostgresDataType {
        return .int8Range
    }
    
    public init?(postgresData: PostgresData) {
        guard let range: Range<Int64> = postgresData.int8Range else {
            return nil
        }
        self = range
    }

    public var postgresData: PostgresData? {
        return .init(int8Range: self)
    }
}

extension ClosedRange: PostgresDataConvertible where Bound == Int64 {
    public static var postgresDataType: PostgresDataType {
        return .int8Range
    }
    
    public init?(postgresData: PostgresData) {
        guard let range: Range<Int64> = postgresData.int8Range else {
            return nil
        }
        self = ClosedRange(range)
    }

    public var postgresData: PostgresData? {
        return .init(int8Range: Range(self))
    }
}
