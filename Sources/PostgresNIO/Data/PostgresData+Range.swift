import NIOCore

// MARK: PostgresRange

@usableFromInline
struct PostgresRange<B> {
    @usableFromInline let lowerBound: B?
    @usableFromInline let upperBound: B?
    @usableFromInline let isLowerBoundInclusive: Bool
    @usableFromInline let isUpperBoundInclusive: Bool

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

// MARK: Private
private let _isLowerBoundInclusive: UInt8 = 0x02
private let _isUpperBoundInclusive: UInt8 = 0x04

