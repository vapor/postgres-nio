import NIOCore
import struct Foundation.Decimal

extension Decimal: PostgresEncodable {
    public static var psqlType: PostgresDataType {
        .numeric
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        let numeric = PostgresNumeric(decimal: self)
        byteBuffer.writeInteger(numeric.ndigits)
        byteBuffer.writeInteger(numeric.weight)
        byteBuffer.writeInteger(numeric.sign)
        byteBuffer.writeInteger(numeric.dscale)
        var value = numeric.value
        byteBuffer.writeBuffer(&value)
    }
}

extension Decimal: PostgresDecodable {
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .numeric):
            guard let numeric = PostgresNumeric(buffer: &buffer) else {
                throw PostgresDecodingError.Code.failure
            }
            self = numeric.decimal
        case (.text, .numeric):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Decimal(string: string) else {
                throw PostgresDecodingError.Code.failure
            }
            self = value
        default:
            throw PostgresDecodingError.Code.typeMismatch
        }
    }
}
