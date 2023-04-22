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
    static var defaultBoundValueForEmptyRange: Self { get }
}

// MARK: Bound conformances

extension FixedWidthInteger {
    public func upperBoundExclusiveToUpperBoundInclusive() -> Self {
        return self - 1
    }

    public static var defaultBoundValueForEmptyRange: Self {
        return .zero
    }
}

extension Int32: PostgresRangeEncodable {
    public static var psqlRangeType: PostgresDataType { return .int4Range }
}

extension Int32: PostgresRangeDecodable {}

extension Int64: PostgresRangeEncodable {
    public static var psqlRangeType: PostgresDataType { return .int8Range }
}

extension Int64: PostgresRangeDecodable {}

// MARK: PostgresRange

@usableFromInline
struct PostgresRange<B> {
    @usableFromInline let lowerBound: B?
    @usableFromInline let upperBound: B?
    @usableFromInline let isLowerBoundInclusive: Bool
    @usableFromInline let isUpperBoundInclusive: Bool

    @usableFromInline
    init(
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
}

extension PostgresRange: PostgresDecodable where B: PostgresRangeDecodable {
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

        let isEmpty: Bool = flags & _isEmpty != 0
        if isEmpty {
            self = PostgresRange<B>(
                lowerBound: B.defaultBoundValueForEmptyRange,
                upperBound: B.defaultBoundValueForEmptyRange,
                isLowerBoundInclusive: true,
                isUpperBoundInclusive: false
            )
            return
        }

        guard let lowerBoundSize: Int32 = byteBuffer.readInteger(as: Int32.self),
            Int(lowerBoundSize) == MemoryLayout<B>.size,
            var lowerBoundBytes: ByteBuffer = byteBuffer.readSlice(length: Int(lowerBoundSize))
        else {
            throw PostgresDecodingError.Code.failure
        }

        let lowerBound: B = try B(from: &lowerBoundBytes, type: boundType, format: format, context: context)

        guard let upperBoundSize = byteBuffer.readInteger(as: Int32.self),
            Int(upperBoundSize) == MemoryLayout<B>.size,
            var upperBoundBytes: ByteBuffer = byteBuffer.readSlice(length: Int(upperBoundSize))
        else {
            throw PostgresDecodingError.Code.failure
        }

        let upperBound: B = try B(from: &upperBoundBytes, type: boundType, format: format, context: context)

        let isLowerBoundInclusive: Bool = flags & _isLowerBoundInclusive != 0
        let isUpperBoundInclusive: Bool = flags & _isUpperBoundInclusive != 0

        self = PostgresRange<B>(
            lowerBound: lowerBound,
            upperBound: upperBound,
            isLowerBoundInclusive: isLowerBoundInclusive,
            isUpperBoundInclusive: isUpperBoundInclusive
        )

    }
}

extension PostgresRange: PostgresEncodable & PostgresNonThrowingEncodable where B: PostgresNonThrowingEncodable {
    @usableFromInline
    static var psqlType: PostgresDataType { return B.psqlType.rangeType! }
    
    @usableFromInline
    static var psqlFormat: PostgresFormat { return .binary }

    @inlinable
    func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) {
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
        if let lowerBound: B = self.lowerBound {
            byteBuffer.writeInteger(boundMemorySize)
            lowerBound.encode(into: &byteBuffer, context: context)
        }
        if let upperBound: B = self.upperBound {
            byteBuffer.writeInteger(boundMemorySize)
            upperBound.encode(into: &byteBuffer, context: context)
        }
    }
}

extension PostgresRange where B: Comparable {
    @inlinable
    init(range: Range<B>) {
        self.lowerBound = range.lowerBound
        self.upperBound = range.upperBound
        self.isLowerBoundInclusive = true
        self.isUpperBoundInclusive = false
    }

    @inlinable
    init(closedRange: ClosedRange<B>) {
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
        
        self = lowerBound...upperBound
    }
}

// MARK: Private
@usableFromInline let _isEmpty: UInt8 = 0x01
@usableFromInline let _isLowerBoundInclusive: UInt8 = 0x02
@usableFromInline let _isUpperBoundInclusive: UInt8 = 0x04
