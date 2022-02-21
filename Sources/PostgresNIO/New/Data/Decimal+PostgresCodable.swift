import NIOCore
import struct Foundation.Decimal

extension Decimal: PostgresCodable {
    public var psqlType: PostgresDataType {
        .numeric
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }
    
    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Decimal {
        switch (format, type) {
        case (.binary, .numeric):
            guard let numeric = PostgresNumeric(buffer: &buffer) else {
                throw PostgresCastingError.Code.failure
            }
            return numeric.decimal
        case (.text, .numeric):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Decimal(string: string) else {
                throw PostgresCastingError.Code.failure
            }
            return value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) {
        let numeric = PostgresNumeric(decimal: self)
        buffer.writeInteger(numeric.ndigits)
        buffer.writeInteger(numeric.weight)
        buffer.writeInteger(numeric.sign)
        buffer.writeInteger(numeric.dscale)
        var value = numeric.value
        buffer.writeBuffer(&value)
    }
}
