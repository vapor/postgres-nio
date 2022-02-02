import struct Foundation.Data
import NIOCore
import NIOFoundationCompat

extension PSQLEncodable where Self: Sequence, Self.Element == UInt8 {
    var psqlType: PSQLDataType {
        .bytea
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeBytes(self)
    }
}

extension ByteBuffer: PSQLCodable {
    var psqlType: PSQLDataType {
        .bytea
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        var copyOfSelf = self // dirty hack
        byteBuffer.writeBuffer(&copyOfSelf)
    }
    
    static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> ByteBuffer {
        return buffer
    }
}

extension Data: PSQLCodable {
    var psqlType: PSQLDataType {
        .bytea
    }

    var psqlFormat: PSQLFormat {
        .binary
    }

    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) {
        byteBuffer.writeBytes(self)
    }

    static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        return buffer.readData(length: buffer.readableBytes, byteTransferStrategy: .automatic)!
    }
}
