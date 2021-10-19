import NIOCore
import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

public protocol PSQLJSONEncoder {
    func encode<T: Encodable>(_ value: T, into buffer: inout ByteBuffer) throws
}

public protocol PSQLJSONDecoder {
    func decode<T: Decodable>(_ type: T.Type, from buffer: ByteBuffer) throws -> T
}

extension JSONEncoder: PSQLJSONEncoder {}
extension JSONDecoder: PSQLJSONDecoder {}

