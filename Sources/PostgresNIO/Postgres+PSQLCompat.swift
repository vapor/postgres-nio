import NIOCore

extension PostgresData: PostgresEncodable {
    var psqlType: PostgresDataType {
        self.type
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }

    func encode<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) throws {
        preconditionFailure("Should never be hit, since `encodeRaw` is implemented.")
    }
    
    // encoding
    func encodeRaw<JSONEncoder: PostgresJSONEncoder>(
        into byteBuffer: inout ByteBuffer,
        context: PostgresEncodingContext<JSONEncoder>
    ) {
        switch self.value {
        case .none:
            byteBuffer.writeInteger(-1, as: Int32.self)
        case .some(var input):
            byteBuffer.writeInteger(Int32(input.readableBytes))
            byteBuffer.writeBuffer(&input)
        }
    }
}

extension PostgresData: PostgresDecodable {
    static func decode<JSONDecoder: PostgresJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PostgresDataType,
        format: PostgresFormat,
        context: PostgresDecodingContext<JSONDecoder>
    ) throws -> Self {
        let myBuffer = buffer.readSlice(length: buffer.readableBytes)!
        
        return PostgresData(type: type, typeModifier: nil, formatCode: .binary, value: myBuffer)
    }
}

extension PostgresData: PostgresCodable {}

extension PSQLError {
    func toPostgresError() -> Error {
        switch self.base {
        case .server(let errorMessage):
            var fields = [PostgresMessage.Error.Field: String]()
            fields.reserveCapacity(errorMessage.fields.count)
            errorMessage.fields.forEach { (key, value) in
                fields[PostgresMessage.Error.Field(rawValue: key.rawValue)!] = value
            }
            return PostgresError.server(PostgresMessage.Error(fields: fields))
        case .sslUnsupported:
            return PostgresError.protocol("Server does not support TLS")
        case .failedToAddSSLHandler(underlying: let underlying):
            return underlying
        case .decoding(let decodingError):
            return PostgresError.protocol("Error decoding message: \(decodingError)")
        case .unexpectedBackendMessage(let message):
            return PostgresError.protocol("Unexpected message: \(message)")
        case .unsupportedAuthMechanism(let authScheme):
            return PostgresError.protocol("Unsupported auth scheme: \(authScheme)")
        case .authMechanismRequiresPassword:
            return PostgresError.protocol("Unable to authenticate without password")
        case .saslError(underlyingError: let underlying):
            return underlying
        case .tooManyParameters:
            return self
        case .connectionQuiescing:
            return PostgresError.connectionClosed
        case .connectionClosed:
            return PostgresError.connectionClosed
        case .connectionError(underlying: let underlying):
            return underlying
        case .casting(let castingError):
            return castingError
        case .uncleanShutdown:
            return PostgresError.protocol("Unexpected connection close")
        }
    }
}

extension PostgresFormat {
    init(psqlFormatCode: PostgresFormat) {
        switch psqlFormatCode {
        case .binary:
            self = .binary
        case .text:
            self = .text
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
