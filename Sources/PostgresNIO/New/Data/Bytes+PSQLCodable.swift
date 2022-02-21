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
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) {
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
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) {
        var copyOfSelf = self // dirty hack
        byteBuffer.writeBuffer(&copyOfSelf)
    }

    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
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

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeBytes(self)
    }

    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        return buffer.readData(length: buffer.readableBytes, byteTransferStrategy: .automatic)!
    }
}
