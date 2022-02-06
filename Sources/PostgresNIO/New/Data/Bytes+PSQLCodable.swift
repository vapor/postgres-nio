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
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        buffer.writeBytes(self)
    }
}

extension ByteBuffer: PSQLCodable {
    public var psqlType: PSQLDataType {
        .bytea
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        var copyOfSelf = self // dirty hack
        buffer.writeBuffer(&copyOfSelf)
    }
    
    public static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> ByteBuffer {
        return buffer
    }
}

extension Data: PSQLCodable {
    public var psqlType: PSQLDataType {
        .bytea
    }

    public var psqlFormat: PSQLFormat {
        .binary
    }

    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        buffer.writeBytes(self)
    }

    public static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        return buffer.readData(length: buffer.readableBytes, byteTransferStrategy: .automatic)!
    }
}
