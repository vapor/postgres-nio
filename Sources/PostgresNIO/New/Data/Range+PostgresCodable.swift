import NIOCore

extension RangeExpression where Bound: PostgresRangeBound {
    public static var psqlFormat: PostgresFormat { return .binary }
}

// MARK: Range

extension Range: PostgresEncodable where Bound: PostgresRangeBound {
    public static var psqlType: PostgresDataType { return Bound.rangeType }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let postgresRange = PostgresRange<Bound>(range: self)
        postgresRange.encode(into: &byteBuffer, context: context)
    }
}

extension Range: PostgresNonThrowingEncodable where Bound: PostgresRangeBound {}

extension Range: PostgresDecodable where Bound: PostgresRangeBound {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard type == Bound.rangeType else {
            throw PostgresDecodingError.Code.typeMismatch
        }

        let postgresRange = try PostgresRange<Bound>(
            from: &buffer,
            type: type,
            format: format,
            context: context
        )

        guard let lowerBound: Bound = postgresRange.lowerBound,
            let upperBound: Bound = postgresRange.upperBound
        else {
            throw PostgresDecodingError.Code.failure
        }
        
        self = lowerBound..<upperBound
    }
}

// MARK: ClosedRange

extension ClosedRange: PostgresEncodable where Bound: PostgresRangeBound {
    public static var psqlType: PostgresDataType { return Bound.rangeType }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let postgresRange = PostgresRange<Bound>(closedRange: self)
        postgresRange.encode(into: &byteBuffer, context: context)
    }
}

extension ClosedRange: PostgresNonThrowingEncodable where Bound: PostgresRangeBound {}

extension ClosedRange: PostgresDecodable where Bound: PostgresRangeBound {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard type == Bound.rangeType else {
            throw PostgresDecodingError.Code.typeMismatch
        }

        let postgresRange = try PostgresRange<Bound>(
            from: &buffer,
            type: type,
            format: format,
            context: context
        )

        guard let lowerBound: Bound = postgresRange.lowerBound,
            var upperBound: Bound = postgresRange.upperBound
        else {
            throw PostgresDecodingError.Code.failure
        }

        if !postgresRange.isUpperBoundInclusive,
            let steppedDownUpperBound = upperBound.stepDown?()
        {
            upperBound = steppedDownUpperBound
        }
        
        self = lowerBound...upperBound
    }
}