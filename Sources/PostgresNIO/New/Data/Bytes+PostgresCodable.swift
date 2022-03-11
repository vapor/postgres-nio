import struct Foundation.Data
import NIOCore
import NIOFoundationCompat

extension PostgresEncodable where Self: Sequence, Self.Element == UInt8 {
    public static var psqlType: PostgresDataType {
        .bytea
    }
    
    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeBytes(self)
    }
}

extension ByteBuffer: PostgresEncodable {
    public static var psqlType: PostgresDataType {
        .bytea
    }
    
    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        var copyOfSelf = self // dirty hack
        byteBuffer.writeBuffer(&copyOfSelf)
    }
}

extension ByteBuffer: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) {
        self = buffer
    }
}

extension ByteBuffer: PostgresCodable {}

extension Data: PostgresEncodable {
    public static var psqlType: PostgresDataType {
        .bytea
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeBytes(self)
    }
}

extension Data: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) {
        self = buffer.readData(length: buffer.readableBytes, byteTransferStrategy: .automatic)!
    }
}

extension Data: PostgresCodable {}
