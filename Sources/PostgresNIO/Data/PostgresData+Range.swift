import NIOCore

// MARK: Postgres range types
public protocol PostgresRangeExpression: PostgresDataConvertible {
    associatedtype Bound: PostgresRangeBound

    var postgresLowerBound: Bound? { get }
    var postgresUpperBound: Bound? { get }
    var isLowerBoundInclusive: Bool { get }
    var isUpperBoundInclusive: Bool { get }
}

public protocol PostgresRangeBound: PostgresDataConvertible {}

public protocol PostgresInt8RangeExpression: CustomStringConvertible {
    var postgresLowerBound: Int64? { get }
    var postgresUpperBound: Int64? { get }
}

// MARK: PostgresData

extension PostgresData {
    public init<R: PostgresRangeExpression>(range: R) {
        var buffer: ByteBuffer = ByteBuffer()

        // flags byte contains certain properties of the range
        var flags: UInt8 = 0
        if range.isLowerBoundInclusive {
            flags |= _isLowerBoundInclusive
        }
        if range.isUpperBoundInclusive {
            flags |= _isUpperBoundInclusive
        }

        let boundMemorySize = Int32(MemoryLayout<R.Bound>.size)

        buffer.writeInteger(flags)
        if var lowerBoundValue: ByteBuffer = range.postgresLowerBound.postgresData?.value {
            buffer.writeInteger(boundMemorySize)
            buffer.writeBuffer(&lowerBoundValue)
        }
        if var upperBoundValue: ByteBuffer = range.postgresUpperBound.postgresData?.value {
            buffer.writeInteger(boundMemorySize)
            buffer.writeBuffer(&upperBoundValue)
        }    
        self.init(type: .int8Range, formatCode: .binary, value: buffer)
    }
    
    public var int8Range: PostgresInt8RangeExpression? {
        guard var value: ByteBuffer = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .int8Range:
                // flags byte contains certain properties of the range
                guard let flags: UInt8 = value.readInteger(as: UInt8.self) else {
                    return nil
                }

                guard let lowerBoundSize = value.readInteger(as: Int32.self),
                    Int(lowerBoundSize) == MemoryLayout<Int64>.size
                else {
                    return nil
                }

                guard let lowerBound: Int64 = value.readInteger(as: Int64.self) else {
                    return nil
                }

                guard let upperBoundSize = value.readInteger(as: Int32.self),
                    Int(upperBoundSize) == MemoryLayout<Int64>.size
                else {
                    return nil
                }

                guard let upperBound: Int64 = value.readInteger(as: Int64.self) else {
                    return nil
                }

                if flags & _isLowerBoundInclusive != 0,
                    flags & _isUpperBoundInclusive == 0
                {
                    return lowerBound..<upperBound
                } else {
                    return nil
                }
                
            default:
                return nil
            }
        case .text:
            return nil
        }
    }
}

extension PostgresInt8RangeExpression where Self: PostgresRangeExpression {
    public static var postgresDataType: PostgresDataType {
        return .int8Range
    }

    public var postgresData: PostgresData? {
        return .init(range: self)
    }
}

// MARK: Swift representations of Postgres type int8range

extension Range: PostgresRangeExpression where Bound == Int64 {
    public var isLowerBoundInclusive: Bool {
        return true
    }

    public var isUpperBoundInclusive: Bool {
        return false
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

extension ClosedRange: PostgresRangeExpression where Bound == Int64 {
    public var isLowerBoundInclusive: Bool {
        return true
    }

    public var isUpperBoundInclusive: Bool {
        return true
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

extension Int64: PostgresRangeBound {}

// MARK: Private
private let _isLowerBoundInclusive: UInt8 = 0x02
private let _isUpperBoundInclusive: UInt8 = 0x04