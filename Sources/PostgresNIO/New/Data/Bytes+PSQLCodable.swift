import struct Foundation.Data
import NIOCore
import NIOFoundationCompat

extension PSQLEncodable where Self: Sequence, Self.Element == UInt8 {
    var psqlType: PostgresDataType {
        .bytea
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeBytes(self)
    }
}

extension ByteBuffer: PSQLCodable {
    var psqlType: PostgresDataType {
        .bytea
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        var copyOfSelf = self // dirty hack
        byteBuffer.writeBuffer(&copyOfSelf)
    }

    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        return buffer
    }
}

extension Data: PSQLCodable {
    var psqlType: PostgresDataType {
        .bytea
    }

    var psqlFormat: PostgresFormat {
        .binary
    }

    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeBytes(self)
    }

    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        return buffer.readData(length: buffer.readableBytes, byteTransferStrategy: .automatic)!
    }
}
