import NIOCore

extension PostgresData: PSQLEncodable {
    public var psqlType: PostgresDataType {
        self.type
    }
    
    public var psqlFormat: PostgresFormat {
        .binary
    }

    public func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) throws {
        preconditionFailure("Should never be hit, since `encodeRaw` is implemented.")
    }
    
    // encoding
    public func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
        into buffer: inout ByteBuffer,
        context: PSQLEncodingContext<JSONEncoder>
    ) {
        switch self.value {
        case .none:
            buffer.writeInteger(-1, as: Int32.self)
        case .some(var input):
            buffer.writeInteger(Int32(input.readableBytes))
            buffer.writeBuffer(&input)
        }
    }
}

extension PostgresData: PostgresDecodable {
    public static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        let myBuffer = buffer.readSlice(length: buffer.readableBytes)!
        
        return PostgresData(type: PostgresDataType(UInt32(type.rawValue)), typeModifier: nil, formatCode: .binary, value: myBuffer)
    }
}

extension PostgresData: PSQLCodable {}

extension PSQLError {
    func toPostgresError() -> Error {
        switch self.code.base {
        case .server(let errorMessage):
            var fields = [PostgresMessage.Error.Field: String]()
            fields.reserveCapacity(errorMessage.fields.count)
            errorMessage.fields.forEach { (key, value) in
                fields[PostgresMessage.Error.Field(rawValue: key.rawValue)!] = value
            }
            return PostgresError.server(PostgresMessage.Error(fields: fields))
        case .sslUnsupported:
            return PostgresError.protocol("Server does not support TLS")
        case .failedToAddSSLHandler:
            return self.underlying ?? self
        case .decoding(let decodingError):
            return PostgresError.protocol("Error decoding message: \(decodingError)")
        case .unexpectedBackendMessage(let message):
            return PostgresError.protocol("Unexpected message: \(message)")
        case .unsupportedAuthMechanism(let authScheme):
            return PostgresError.protocol("Unsupported auth scheme: \(authScheme)")
        case .authMechanismRequiresPassword:
            return PostgresError.protocol("Unable to authenticate without password")
        case .saslError:
            return self.underlying ?? self
        case .tooManyParameters:
            return self
        case .connectionQuiescing:
            return PostgresError.connectionClosed
        case .connectionClosed:
            return PostgresError.connectionClosed
        case .connectionError:
            return self.underlying ?? self
        case .uncleanShutdown:
            return PostgresError.protocol("Unexpected connection close")
        }
    }
}

extension Error {
    internal var asAppropriatePostgresError: Error {
        if let psqlError = self as? PSQLError {
            return psqlError.toPostgresError()
        } else {
            return self
        }
    }
}
