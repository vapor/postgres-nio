import NIOCore

// MARK: UInt8

extension UInt8: PostgresEncodable {
    static var psqlType: PostgresDataType {
        .char
    }

    static var psqlFormat: PostgresFormat {
        .binary
    }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self, as: UInt8.self)
    }
}

extension UInt8: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch type {
        case .bpchar, .char:
            guard buffer.readableBytes == 1, let value = buffer.readInteger(as: UInt8.self) else {
                throw PostgresCastingError.Code.failure
            }

            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension UInt8: PostgresCodable {}

// MARK: Int16

extension Int16: PostgresEncodable {
    static var psqlType: PostgresDataType {
        .int2
    }

    static var psqlFormat: PostgresFormat {
        .binary
    }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self, as: Int16.self)
    }
}

extension Int16: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        case (.text, .int2):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int16(string) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Int16: PostgresCodable {}

// MARK: Int32

extension Int32: PostgresEncodable {
    static var psqlType: PostgresDataType {
        .int4
    }
    
    static var psqlFormat: PostgresFormat {
        .binary
    }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self, as: Int32.self)
    }
}

extension Int32: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = Int32(value)
        case (.binary, .int4):
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = Int32(value)
        case (.text, .int2), (.text, .int4):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int32(string) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Int32: PostgresCodable {}

// MARK: Int64

extension Int64: PostgresEncodable {
    static var psqlType: PostgresDataType {
        .int8
    }

    static var psqlFormat: PostgresFormat {
        .binary
    }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self, as: Int64.self)
    }
}

extension Int64: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = Int64(value)
        case (.binary, .int4):
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = Int64(value)
        case (.binary, .int8):
            guard buffer.readableBytes == 8, let value = buffer.readInteger(as: Int64.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        case (.text, .int2), (.text, .int4), (.text, .int8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int64(string) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Int64: PostgresCodable {}

// MARK: Int

extension Int: PostgresEncodable {
    static var psqlType: PostgresDataType {
        switch self.bitWidth {
        case Int32.bitWidth:
            return .int4
        case Int64.bitWidth:
            return .int8
        default:
            preconditionFailure("Int is expected to be an Int32 or Int64")
        }
    }
    
    static var psqlFormat: PostgresFormat {
        .binary
    }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        byteBuffer.writeInteger(self, as: Int.self)
    }
}

extension Int: PostgresDecodable {
    @inlinable
    public init<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws {
        switch (format, type) {
        case (.binary, .int2):
            guard buffer.readableBytes == 2, let value = buffer.readInteger(as: Int16.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = Int(value)
        case (.binary, .int4):
            guard buffer.readableBytes == 4, let value = buffer.readInteger(as: Int32.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = Int(value)
        case (.binary, .int8) where Int.bitWidth == 64:
            guard buffer.readableBytes == 8, let value = buffer.readInteger(as: Int.self) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        case (.text, .int2), (.text, .int4), (.text, .int8):
            guard let string = buffer.readString(length: buffer.readableBytes), let value = Int(string) else {
                throw PostgresCastingError.Code.failure
            }
            self = value
        default:
            throw PostgresCastingError.Code.typeMismatch
        }
    }
}

extension Int: PostgresCodable {}
