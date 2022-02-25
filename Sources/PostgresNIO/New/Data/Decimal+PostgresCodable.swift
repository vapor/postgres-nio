import NIOCore
import struct Foundation.Decimal

extension Decimal: PostgresCodable {
    var psqlType: PostgresDataType {
        .numeric
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
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
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
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
