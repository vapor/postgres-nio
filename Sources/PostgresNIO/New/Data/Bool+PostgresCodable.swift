import NIOCore

extension Bool: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        guard type == .bool else {
            throw PostgresDecodingError.Code.typeMismatch
        }

        switch format {
        case .binary:
            guard buffer.readableBytes == 1 else {
                throw PostgresDecodingError.Code.failure
            }

            switch buffer.readInteger(as: UInt8.self) {
            case .some(0):
                self = false
            case .some(1):
                self = true
            default:
                throw PostgresDecodingError.Code.failure
            }
        case .text:
            guard buffer.readableBytes == 1 else {
                throw PostgresDecodingError.Code.failure
            }

            switch buffer.readInteger(as: UInt8.self) {
            case .some(UInt8(ascii: "f")):
                self = false
            case .some(UInt8(ascii: "t")):
                self = true
            default:
                throw PostgresDecodingError.Code.failure
            }
        }
    }
}

extension Bool: PostgresEncodable {
    public static var psqlType: PostgresDataType {
        .bool
    }

    public static var psqlFormat: PostgresFormat {
        .binary
    }

    @inlinable
    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}
