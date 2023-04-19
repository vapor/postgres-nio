import NIOCore

// MARK: PostgresRangeBound

public protocol PostgresRangeBound: PostgresDecodable, PostgresNonThrowingEncodable {
    /// the Postgres range type associated with this bound
    static var rangeType: PostgresDataType { get }
    
    /// If a Postgres range type has a well-defined step,
    /// Postgres automatically converts it to a canonical form.
    /// Types such as `int4range` get converted to upper-bound-exclusive.
    /// This method is needed when converting an upper bound to inclusive.
    /// It should throw if the type lacks a well-defined step.
    func stepDown() throws -> Self
}

extension Int32: PostgresRangeBound {
    public static var rangeType: PostgresDataType { return .int4Range }

    public func stepDown() -> Self {
        return self - 1
    }
}

extension Int64: PostgresRangeBound {
    public static var rangeType: PostgresDataType { return .int8Range }

    public func stepDown() -> Self {
        return self - 1
    }
}

// MARK: PostgresRange

public struct PostgresRange<B: PostgresRangeBound> {
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
}

extension PostgresRange: PostgresDecodable {
    public init<JSONDecoder: PostgresJSONDecoder>(
        from byteBuffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard case .binary = format else {
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

        let lowerBound: B = try B(from: &lowerBoundBytes, type: B.psqlType, format: format, context: context)

        guard let upperBoundSize = byteBuffer.readInteger(as: Int32.self),
            Int(upperBoundSize) == MemoryLayout<B>.size,
            var upperBoundBytes: ByteBuffer = byteBuffer.readSlice(length: Int(upperBoundSize))
        else {
            throw PostgresDecodingError.Code.failure
        }

        let upperBound: B = try B(from: &upperBoundBytes, type: B.psqlType, format: format, context: context)

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

extension PostgresRange: PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType { return B.rangeType }
    
    public static var psqlFormat: PostgresFormat { return .binary }

    public func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) {
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

extension PostgresRange: CustomStringConvertible {
    public var description: String {
        let opener: Character = self.isLowerBoundInclusive ? "[" : "("
        let closer: Character = self.isUpperBoundInclusive ? "]" : ")"
        let lowerBoundString: String = self.lowerBound == nil ? "" : "\(self.lowerBound!)"
        let upperBoundString: String = self.upperBound == nil ? "" : "\(self.upperBound!)"
        return "\(opener)\(lowerBoundString),\(upperBoundString),\(closer)"
    }
}

extension PostgresRange where B: PostgresRangeBound & Comparable {
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
}

// MARK: Private
private let _isLowerBoundInclusive: UInt8 = 0x02
private let _isUpperBoundInclusive: UInt8 = 0x04

