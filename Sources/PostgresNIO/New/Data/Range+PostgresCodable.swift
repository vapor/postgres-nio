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
    func stepDown() throws -> Self
}

// MARK: Bound conformances

extension Int32: PostgresRangeEncodable {
    public static var psqlRangeType: PostgresDataType { return .int4Range }
}

extension Int32: PostgresRangeDecodable {
    public func stepDown() -> Self {
        return self - 1
    }
}

extension Int64: PostgresRangeEncodable {
    public static var psqlRangeType: PostgresDataType { return .int8Range }
}

extension Int64: PostgresRangeDecodable {
    public func stepDown() -> Self {
        return self - 1
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
            upperBound = try upperBound.stepDown()
        }
        
        self = lowerBound...upperBound
    }
}