import NIOCore
import Foundation.NSDecimal

extension Decimal: PSQLCodable {
    var psqlType: PSQLDataType {
        .numeric
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    static func decode(from byteBuffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Decimal {
        switch (format, type) {
        case (.binary, .numeric):
            guard let numeric = PostgresNumeric(buffer: &byteBuffer) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: byteBuffer, context: context)
            }
            return numeric.decimal
        case (.text, .numeric):
            guard let string = byteBuffer.readString(length: byteBuffer.readableBytes), let value = Decimal(string: string) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: byteBuffer, context: context)
            }
            return value
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: byteBuffer, context: context)
        }
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        let numeric = PostgresNumeric(decimal: self)
        byteBuffer.writeInteger(numeric.ndigits)
        byteBuffer.writeInteger(numeric.weight)
        byteBuffer.writeInteger(numeric.sign)
        byteBuffer.writeInteger(numeric.dscale)
        var value = numeric.value
        byteBuffer.writeBuffer(&value)
    }
}
