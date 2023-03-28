import NIOCore

// MARK: PostgresRangeBound

public protocol PostgresRangeBound: PostgresDataConvertible, Comparable {
    /// the Postgres range type associated with this bound
    static var rangeType: PostgresDataType { get }
    
    /// If a Postgres range type has a well-defined step,
    /// Postgres automatically converts it to a canonical form.
    /// Types such as `int4range` get converted to upper-bound-exclusive.
    /// This method is needed when converting an upper bound to inclusive.
    var stepDown: (() -> Self)? { get }
}

extension Int32: PostgresRangeBound {
    public static var rangeType: PostgresDataType { return .int4Range }

    public var stepDown: (() -> Int32)? {
        { self - 1 }
    }
}

extension Int64: PostgresRangeBound {
    public static var rangeType: PostgresDataType { return .int8Range }

    public var stepDown: (() -> Int64)? {
        { self - 1 }
    }
}

// MARK: PostgresRange

public struct PostgresRange<B: PostgresRangeBound>: CustomStringConvertible {
    public let lowerBound: B?
    public let upperBound: B?
    public let isLowerBoundInclusive: Bool
    public let isUpperBoundInclusive: Bool

    public init(
        lowerBound: B?,
        upperBound: B?,
        isLowerBoundInclusive: Bool,
        isUpperBoundInclusive: Bool
    ) {
        self.lowerBound = lowerBound
        self.upperBound = upperBound
        self.isLowerBoundInclusive = isLowerBoundInclusive
        self.isUpperBoundInclusive = isUpperBoundInclusive
    }

    public init(range: Range<B>) {
        self.lowerBound = range.lowerBound
        self.upperBound = range.upperBound
        self.isLowerBoundInclusive = true
        self.isUpperBoundInclusive = false
    }

    public init(closedRange: ClosedRange<B>) {
        self.lowerBound = closedRange.lowerBound
        self.upperBound = closedRange.upperBound
        self.isLowerBoundInclusive = true
        self.isUpperBoundInclusive = true
    }

    public init?(
        from buffer: inout ByteBuffer,
        format: PostgresFormat
    ) {
        guard case .binary = format else {
            return nil
        }

        // flags byte contains certain properties of the range
        guard let flags: UInt8 = buffer.readInteger(as: UInt8.self) else {
            return nil
        }

        guard let lowerBoundSize: Int32 = buffer.readInteger(as: Int32.self),
            Int(lowerBoundSize) == MemoryLayout<B>.size
        else {
            return nil
        }

        guard let lowerBoundBytes = buffer.readSlice(length: Int(lowerBoundSize)),
            let lowerBound: B = B(postgresData: PostgresData(type: B.postgresDataType, formatCode: format, value: lowerBoundBytes))
        else {
            return nil
        }

        guard let upperBoundSize = buffer.readInteger(as: Int32.self),
            Int(upperBoundSize) == MemoryLayout<B>.size
        else {
            return nil
        }

        guard let upperBoundBytes = buffer.readSlice(length: Int(lowerBoundSize)),
            let upperBound: B = B(postgresData: PostgresData(type: B.postgresDataType, formatCode: format, value: upperBoundBytes))
        else {
            return nil
        }

        let isLowerBoundInclusive: Bool = flags & _isLowerBoundInclusive != 0
        let isUpperBoundInclusive: Bool = flags & _isUpperBoundInclusive != 0

        self = PostgresRange<B>(
            lowerBound: lowerBound,
            upperBound: upperBound,
            isLowerBoundInclusive: isLowerBoundInclusive,
            isUpperBoundInclusive: isUpperBoundInclusive
        )

    }

    public func encode(into byteBuffer: inout ByteBuffer) {
        // flags byte contains certain properties of the range
        var flags: UInt8 = 0
        if self.isLowerBoundInclusive {
            flags |= _isLowerBoundInclusive
        }
        if self.isUpperBoundInclusive {
            flags |= _isUpperBoundInclusive
        }

        let boundMemorySize = Int32(MemoryLayout<B>.size)

        byteBuffer.writeInteger(flags)
        if var lowerBoundValue: ByteBuffer = self.lowerBound.postgresData?.value {
            byteBuffer.writeInteger(boundMemorySize)
            byteBuffer.writeBuffer(&lowerBoundValue)
        }
        if var upperBoundValue: ByteBuffer = self.upperBound.postgresData?.value {
            byteBuffer.writeInteger(boundMemorySize)
            byteBuffer.writeBuffer(&upperBoundValue)
        }
    }

    public var description: String {
        let opener: Character = self.isLowerBoundInclusive ? "[" : "("
        let closer: Character = self.isUpperBoundInclusive ? "]" : ")"
        let lowerBoundString: String = self.lowerBound == nil ? "" : "\(self.lowerBound!)"
        let upperBoundString: String = self.upperBound == nil ? "" : "\(self.upperBound!)"
        return "\(opener)\(lowerBoundString),\(upperBoundString),\(closer)"
    }
}

// MARK: PostgresData

extension PostgresData {
    public func range<B>() -> PostgresRange<B>?
        where B: PostgresRangeBound
    {
        guard var value: ByteBuffer = self.value else {
            return nil
        }

        return PostgresRange<B>(
            from: &value,
            format: self.formatCode
        )
    }

    public init<B>(range: PostgresRange<B>)
    {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        range.encode(into: &buffer)

        self.init(
            type: B.rangeType,
            typeModifier: nil,
            formatCode: .binary,
            value: buffer
        )
    }
}

// MARK: PostgresDataConvertible conformance

extension PostgresRange: PostgresDataConvertible where B: PostgresRangeBound {
    public static var postgresDataType: PostgresDataType { return B.rangeType }
    public init?(postgresData: PostgresData) {
        guard var value = postgresData.value else {
            return nil
        }
        self.init(from: &value, format: .binary)
    }
    public var postgresData: PostgresData? {
        return PostgresData(range: self)
    }
}

extension RangeExpression where Bound: PostgresRangeBound {
    public static var postgresDataType: PostgresDataType {
        return Bound.postgresDataType
    }
}

extension Range: PostgresDataConvertible where Bound: PostgresRangeBound {
    public init?(postgresData: PostgresData) {
        guard let postgresRange: PostgresRange<Bound> = postgresData.range(),
            let lowerBound: Bound = postgresRange.lowerBound,
            let upperBound: Bound = postgresRange.upperBound,
            postgresRange.isLowerBoundInclusive,
            !postgresRange.isUpperBoundInclusive
        else {
            return nil
        }

        self = lowerBound..<upperBound
    }

    public var postgresData: PostgresData? {
        return PostgresData(range: PostgresRange<Bound>(range: self))
    }
}

extension ClosedRange: PostgresDataConvertible where Bound: PostgresRangeBound {
    public init?(postgresData: PostgresData) {
        guard let postgresRange: PostgresRange<Bound> = postgresData.range(),
            let lowerBound: Bound = postgresRange.lowerBound,
            var upperBound: Bound = postgresRange.upperBound,
            postgresRange.isLowerBoundInclusive
        else {
            return nil
        }

        if !postgresRange.isUpperBoundInclusive,
            let steppedDownUpperBound = upperBound.stepDown?()
        {
            upperBound = steppedDownUpperBound
        }

        self = lowerBound...upperBound
    }

    public var postgresData: PostgresData? {
        return PostgresData(range: PostgresRange<Bound>.init(closedRange: self))
    }
}

// MARK: Private
private let _isLowerBoundInclusive: UInt8 = 0x02
private let _isUpperBoundInclusive: UInt8 = 0x04

