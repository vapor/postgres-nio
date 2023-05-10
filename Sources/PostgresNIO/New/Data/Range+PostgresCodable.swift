import NIOCore

// MARK: Protocols

/// A type that can be encoded into a Postgres range type where it is the bound type
public protocol PostgresRangeEncodable: PostgresNonThrowingEncodable {
    static var psqlRangeType: PostgresDataType { get }
}

/// A type that can be decoded into a Swift RangeExpression type from a Postgres range where it is the bound type
public protocol PostgresRangeDecodable: PostgresDecodable {
    /// If a Postgres range type has a well-defined step,
    /// Postgres automatically converts it to a canonical form.
    /// Types such as `int4range` get converted to upper-bound-exclusive.
    /// This method is needed when converting an upper bound to inclusive.
    /// It should throw if the type lacks a well-defined step.
    func upperBoundExclusiveToUpperBoundInclusive() throws -> Self

    /// Postgres does not store any bound values for empty ranges,
    /// but Swift requires a value to initialize an empty Range<Bound>.
    static var valueForEmptyRange: Self { get }
}

/// A type that can be encoded into a Postgres range array type where it is the bound type
public protocol PostgresRangeArrayEncodable: PostgresRangeEncodable {
    static var psqlRangeArrayType: PostgresDataType { get }
}

/// A type that can be decoded into a Swift RangeExpression array type from a Postgres range array where it is the bound type
public protocol PostgresRangeArrayDecodable: PostgresRangeDecodable {}

// MARK: Bound conformances

extension FixedWidthInteger where Self: PostgresRangeDecodable {
    public func upperBoundExclusiveToUpperBoundInclusive() -> Self {
        return self - 1
    }

    public static var valueForEmptyRange: Self {
        return .zero
    }
}

extension Int32: PostgresRangeEncodable {
    public static var psqlRangeType: PostgresDataType { return .int4Range }
}

extension Int32: PostgresRangeDecodable {}

extension Int32: PostgresRangeArrayEncodable {
    public static var psqlRangeArrayType: PostgresDataType { return .int4RangeArray }
}

extension Int32: PostgresRangeArrayDecodable {}

extension Int64: PostgresRangeEncodable {
    public static var psqlRangeType: PostgresDataType { return .int8Range }
}

extension Int64: PostgresRangeDecodable {}

extension Int64: PostgresRangeArrayEncodable {
    public static var psqlRangeArrayType: PostgresDataType { return .int8RangeArray }
}

extension Int64: PostgresRangeArrayDecodable {}

// MARK: PostgresRange

@usableFromInline
struct PostgresRange<Bound> {
    @usableFromInline let lowerBound: Bound?
    @usableFromInline let upperBound: Bound?
    @usableFromInline let isLowerBoundInclusive: Bool
    @usableFromInline let isUpperBoundInclusive: Bool

    @inlinable
    init(
        lowerBound: Bound?,
        upperBound: Bound?,
        isLowerBoundInclusive: Bool,
        isUpperBoundInclusive: Bool
    ) {
        self.lowerBound = lowerBound
        self.upperBound = upperBound
        self.isLowerBoundInclusive = isLowerBoundInclusive
        self.isUpperBoundInclusive = isUpperBoundInclusive
    }
}

/// Used by Postgres to represent certain range properties
@usableFromInline
struct PostgresRangeFlag {
    @usableFromInline static let isEmpty: UInt8 = 0x01
    @usableFromInline static let isLowerBoundInclusive: UInt8 = 0x02
    @usableFromInline static let isUpperBoundInclusive: UInt8 = 0x04
}

extension PostgresRange: PostgresDecodable where Bound: PostgresRangeDecodable {
    @inlinable
    init<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard case .binary = format else {
            throw PostgresDecodingError.Code.failure
        }

        guard let boundType: PostgresDataType = type.boundType else {
            throw PostgresDecodingError.Code.failure
        }

        // flags byte contains certain properties of the range
        guard let flags: UInt8 = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        let isEmpty: Bool = flags & PostgresRangeFlag.isEmpty != 0
        if isEmpty {
            self = PostgresRange(
                lowerBound: Bound.valueForEmptyRange,
                upperBound: Bound.valueForEmptyRange,
                isLowerBoundInclusive: true,
                isUpperBoundInclusive: false
            )
            return
        }

        guard let lowerBoundSize: Int32 = byteBuffer.readInteger(as: Int32.self),
            Int(lowerBoundSize) == MemoryLayout<Bound>.size,
            var lowerBoundBytes: ByteBuffer = byteBuffer.readSlice(length: Int(lowerBoundSize))
        else {
            throw PostgresDecodingError.Code.failure
        }

        let lowerBound = try Bound(from: &lowerBoundBytes, type: boundType, format: format, context: context)

        guard let upperBoundSize = byteBuffer.readInteger(as: Int32.self),
            Int(upperBoundSize) == MemoryLayout<Bound>.size,
            var upperBoundBytes: ByteBuffer = byteBuffer.readSlice(length: Int(upperBoundSize))
        else {
            throw PostgresDecodingError.Code.failure
        }

        let upperBound = try Bound(from: &upperBoundBytes, type: boundType, format: format, context: context)

        let isLowerBoundInclusive: Bool = flags & PostgresRangeFlag.isLowerBoundInclusive != 0
        let isUpperBoundInclusive: Bool = flags & PostgresRangeFlag.isUpperBoundInclusive != 0

        self = PostgresRange(
            lowerBound: lowerBound,
            upperBound: upperBound,
            isLowerBoundInclusive: isLowerBoundInclusive,
            isUpperBoundInclusive: isUpperBoundInclusive
        )

    }
}

extension PostgresRange: PostgresEncodable & PostgresNonThrowingEncodable where Bound: PostgresRangeEncodable {
    @usableFromInline
    static var psqlType: PostgresDataType { return Bound.psqlRangeType }

    @usableFromInline
    static var psqlFormat: PostgresFormat { return .binary }

    @inlinable
    func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) {
        // flags byte contains certain properties of the range
        var flags: UInt8 = 0
        if self.isLowerBoundInclusive {
            flags |= PostgresRangeFlag.isLowerBoundInclusive
        }
        if self.isUpperBoundInclusive {
            flags |= PostgresRangeFlag.isUpperBoundInclusive
        }

        let boundMemorySize = Int32(MemoryLayout<Bound>.size)

        byteBuffer.writeInteger(flags)
        if let lowerBound = self.lowerBound {
            byteBuffer.writeInteger(boundMemorySize)
            lowerBound.encode(into: &byteBuffer, context: context)
        }
        if let upperBound = self.upperBound {
            byteBuffer.writeInteger(boundMemorySize)
            upperBound.encode(into: &byteBuffer, context: context)
        }
    }
}

extension PostgresRange where Bound: Comparable {
    @inlinable
    init(range: Range<Bound>) {
        self.lowerBound = range.lowerBound
        self.upperBound = range.upperBound
        self.isLowerBoundInclusive = true
        self.isUpperBoundInclusive = false
    }

    @inlinable
    init(closedRange: ClosedRange<Bound>) {
        self.lowerBound = closedRange.lowerBound
        self.upperBound = closedRange.upperBound
        self.isLowerBoundInclusive = true
        self.isUpperBoundInclusive = true
    }
}

// MARK: Range

extension Range: PostgresEncodable where Bound: PostgresRangeEncodable {
    public static var psqlType: PostgresDataType { return Bound.psqlRangeType }
    public static var psqlFormat: PostgresFormat { return .binary }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let postgresRange = PostgresRange<Bound>(range: self)
        postgresRange.encode(into: &byteBuffer, context: context)
    }
}

extension Range: PostgresNonThrowingEncodable where Bound: PostgresRangeEncodable {}

extension Range: PostgresDecodable where Bound: PostgresRangeDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        let postgresRange = try PostgresRange<Bound>(
            from: &buffer,
            type: type,
            format: format,
            context: context
        )

        guard let lowerBound: Bound = postgresRange.lowerBound,
            let upperBound: Bound = postgresRange.upperBound,
            postgresRange.isLowerBoundInclusive,
            !postgresRange.isUpperBoundInclusive
        else {
            throw PostgresDecodingError.Code.failure
        }
        
        self = lowerBound..<upperBound
    }
}

// MARK: ClosedRange

extension ClosedRange: PostgresEncodable where Bound: PostgresRangeEncodable {
    public static var psqlType: PostgresDataType { return Bound.psqlRangeType }
    public static var psqlFormat: PostgresFormat { return .binary }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let postgresRange = PostgresRange<Bound>(closedRange: self)
        postgresRange.encode(into: &byteBuffer, context: context)
    }
}

extension ClosedRange: PostgresNonThrowingEncodable where Bound: PostgresRangeEncodable {}

extension ClosedRange: PostgresDecodable where Bound: PostgresRangeDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        let postgresRange = try PostgresRange<Bound>(
            from: &buffer,
            type: type,
            format: format,
            context: context
        )

        guard let lowerBound: Bound = postgresRange.lowerBound,
            var upperBound: Bound = postgresRange.upperBound,
            postgresRange.isLowerBoundInclusive
        else {
            throw PostgresDecodingError.Code.failure
        }

        if !postgresRange.isUpperBoundInclusive {
            upperBound = try upperBound.upperBoundExclusiveToUpperBoundInclusive()
        }

        if lowerBound > upperBound {
            throw PostgresDecodingError.Code.failure
        }
        
        self = lowerBound...upperBound
    }
}
