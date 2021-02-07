struct PostgresJSONDecoderWrapper: PSQLJSONDecoder {
    let downstream: PostgresJSONDecoder
    
    init(_ downstream: PostgresJSONDecoder) {
        self.downstream = downstream
    }
    
    func decode<T>(_ type: T.Type, from buffer: ByteBuffer) throws -> T where T : Decodable {
        var buffer = buffer
        let data = buffer.readData(length: buffer.readableBytes)!
        return try self.downstream.decode(T.self, from: data)
    }
}

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
    var psqlType: PSQLDataType {
        PSQLDataType(Int32(self.type.rawValue))
    }
    
    // encoding
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        guard var selfBuffer = self.value else {
            return
        }
        byteBuffer.writeBuffer(&selfBuffer)
    }
}

extension PostgresData: PSQLDecodable {
    static func decode(from byteBuffer: inout ByteBuffer, type: PSQLDataType, context: PSQLDecodingContext) throws -> PostgresData {
        let myBuffer = byteBuffer.readSlice(length: byteBuffer.readableBytes)!
        
        return PostgresData(type: PostgresDataType(UInt32(type.rawValue)), typeModifier: nil, formatCode: .binary, value: myBuffer)
    }
}

extension PostgresData: PSQLCodable {}

public protocol Foo {
    static var foo: Int { get }
}

extension PSQLError {
    func toPostgresError() -> Error {
        switch self.underlying {
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
        }
    }

}
