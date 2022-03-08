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
            throw PostgresCastingError.Code.typeMismatch
        }

        switch format {
        case .binary:
            guard buffer.readableBytes == 1 else {
                throw PostgresCastingError.Code.failure
            }

            switch buffer.readInteger(as: UInt8.self) {
            case .some(0):
                self = false
            case .some(1):
                self = true
            default:
                throw PostgresCastingError.Code.failure
            }
        case .text:
            guard buffer.readableBytes == 1 else {
                throw PostgresCastingError.Code.failure
            }

            switch buffer.readInteger(as: UInt8.self) {
            case .some(UInt8(ascii: "f")):
                self = false
            case .some(UInt8(ascii: "t")):
                self = true
            default:
                throw PostgresCastingError.Code.failure
            }
        }
    }
}

extension Bool: PostgresEncodable {
    var psqlType: PostgresDataType {
        .bool
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    }
}

extension Bool: PostgresCodable {}
