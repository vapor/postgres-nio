import NIOCore

extension Range: PostgresEncodable where Bound == Int64 {
    public static var psqlType: PostgresDataType {
        .int8Range
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(2, as: Int8.self)
        byteBuffer.writeInteger(8, as: Int32.self)
        byteBuffer.writeInteger(self.lowerBound)
        byteBuffer.writeInteger(8, as: Int32.self)
        byteBuffer.writeInteger(self.upperBound)
    }
}

extension Range: PostgresNonThrowingEncodable where Bound == Int64 {}

extension Range: PostgresDecodable where Bound == Int64 {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard type == .int8Range else {
            throw PostgresDecodingError.Code.typeMismatch
        }

        switch format {
        case .binary:
            guard buffer.readableBytes == 25 else {
                throw PostgresDecodingError.Code.failure
            }

            guard buffer.readInteger(as: Int8.self) == 2 else {
                throw PostgresDecodingError.Code.failure
            }

            guard buffer.readInteger(as: Int32.self) == 8 else {
                throw PostgresDecodingError.Code.failure
            }

            guard let lowerBound: Int64 = buffer.readInteger() else {
                throw PostgresDecodingError.Code.failure
            }

            guard buffer.readInteger(as: Int32.self) == 8 else {
                throw PostgresDecodingError.Code.failure
            }

            guard let upperBound: Int64 = buffer.readInteger() else {
                throw PostgresDecodingError.Code.failure
            }

            self = lowerBound..<upperBound
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}

extension ClosedRange: PostgresEncodable where Bound == Int64 {
    public static var psqlType: PostgresDataType {
        .int8Range
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        Range(self).encode(
            into: &byteBuffer,
            context: context
        )
    }
}

extension ClosedRange: PostgresNonThrowingEncodable where Bound == Int64 {}

extension ClosedRange: PostgresDecodable where Bound == Int64 {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        let range = try Range<Int64>(
            from: &buffer,
            type: type,
            format: format,
            context: context
        )

        self = ClosedRange(range)
    }
}