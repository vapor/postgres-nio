import NIOCore

struct PostgresJSONEncoderWrapper: PSQLJSONEncoder {
    let downstream: PostgresJSONEncoder
    
    init(_ downstream: PostgresJSONEncoder) {
        self.downstream = downstream
    }
    
    func encode<T>(_ value: T, into buffer: inout ByteBuffer) throws where T : Encodable {
        let data = try self.downstream.encode(value)
        buffer.writeData(data)
    }
}

extension PostgresData: PSQLEncodable {
    var psqlType: PostgresDataType {
        self.type
    }
    
    var psqlFormat: PostgresFormat {
        .binary
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        preconditionFailure("Should never be hit, since `encodeRaw` is implemented.")
    }
    
    // encoding
    func encodeRaw(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        switch self.value {
        case .none:
            byteBuffer.writeInteger(-1, as: Int32.self)
        case .some(var input):
            byteBuffer.writeInteger(Int32(input.readableBytes))
            byteBuffer.writeBuffer(&input)
        }
    }
}

extension PostgresData: PSQLDecodable {
    static func decode(from buffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PSQLDecodingContext) throws -> Self {
        let myBuffer = buffer.readSlice(length: buffer.readableBytes)!
        
        return PostgresData(type: PostgresDataType(UInt32(type.rawValue)), typeModifier: nil, formatCode: .binary, value: myBuffer)
    }
}

extension PostgresData: PSQLCodable {}

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
