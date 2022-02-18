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
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        buffer.writeBytes(self)
    }
}

extension ByteBuffer: PSQLCodable {
    public var psqlType: PostgresDataType {
        .bytea
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        var copyOfSelf = self // dirty hack
        buffer.writeBuffer(&copyOfSelf)
    }
    
    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> ByteBuffer {
        return buffer
    }
}

extension Data: PSQLCodable {
    public var psqlType: PostgresDataType {
        .bytea
    }

    public var psqlFormat: PostgresFormat {
        .binary
    }

    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        buffer.writeBytes(self)
    }

    public static func decode<JSONDecoder : PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        return buffer.readData(length: buffer.readableBytes, byteTransferStrategy: .automatic)!
    }
}
