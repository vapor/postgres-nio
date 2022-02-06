import NIOCore
import struct Foundation.Decimal

extension Decimal: PSQLCodable {
    public var psqlType: PSQLDataType {
        .numeric
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    public static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Decimal {
        switch (format, type) {
        case (.binary, .numeric):
            guard let numeric = PostgresNumeric(buffer: &buffer) else {
                throw PSQLCastingError.Code.failure
            }
            return numeric.decimal
        case (.text, .numeric):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Decimal(string: string) else {
                throw PSQLCastingError.Code.failure
            }
            return value
        default:
            throw PSQLCastingError.Code.typeMismatch
        }
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) {
        let numeric = PostgresNumeric(decimal: self)
        buffer.writeInteger(numeric.ndigits)
        buffer.writeInteger(numeric.weight)
        buffer.writeInteger(numeric.sign)
        buffer.writeInteger(numeric.dscale)
        var value = numeric.value
        buffer.writeBuffer(&value)
    }
}
