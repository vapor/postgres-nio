import NIOCore

public protocol PostgresInt8RangeExpression: PostgresDataConvertible, CustomStringConvertible {
    var postgresLowerBound: Int64? { get }
    var postgresUpperBound: Int64? { get }
}

extension PostgresInt8RangeExpression {
    public static var postgresDataType: PostgresDataType {
        return .int8Range
    }

    public var postgresData: PostgresData? {
        return .init(int8Range: self)
    }
}

extension PostgresData {
    public init<R: PostgresInt8RangeExpression>(int8Range: R) {
        guard let lowerBound: Int64 = int8Range.postgresLowerBound else {
            fatalError("Unexpected type \(Swift.type(of: int8Range))")
        }

        guard let upperBound: Int64 = int8Range.postgresUpperBound else {
            fatalError("Unexpected type \(Swift.type(of: int8Range))")
        }

        var buffer = ByteBufferAllocator().buffer(capacity: 25)
        
        if int8Range is Range<Int64> {
            buffer.writeInteger(2, as: Int8.self)
        } else if int8Range is ClosedRange<Int64> {
            buffer.writeInteger(6, as: Int8.self)
        } else {
            fatalError("Unexpected type \(Swift.type(of: int8Range))")
        }

        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(lowerBound)
        buffer.writeInteger(8, as: Int32.self)
        buffer.writeInteger(upperBound)
        self.init(type: .int8Range, formatCode: .binary, value: buffer)
    }
    
    public var int8Range: PostgresInt8RangeExpression? {
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

extension Range: PostgresInt8RangeExpression where Bound == Int64 {
    public var postgresLowerBound: Int64? {
        return self.lowerBound
    }

    public var postgresUpperBound: Int64? {
        return self.upperBound
    }
}

extension Range: PostgresDataConvertible where Bound == Int64 {
    public init?(postgresData: PostgresData) {
        guard let range: Range<Int64> = postgresData.int8Range as? Range<Int64> else {
            return nil
        }
        self = range
    }
}

extension ClosedRange: PostgresInt8RangeExpression where Bound == Int64 {
    public var postgresLowerBound: Int64? {
        return self.lowerBound
    }

    public var postgresUpperBound: Int64? {
        return self.upperBound
    }
}

extension ClosedRange: PostgresDataConvertible where Bound == Int64 {
    public init?(postgresData: PostgresData) {
        guard let range: Range<Int64> = postgresData.int8Range as? Range<Int64> else {
            return nil
        }
        self = ClosedRange(range)
    }
}
