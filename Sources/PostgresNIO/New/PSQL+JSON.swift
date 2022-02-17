import NIOCore
import NIOFoundationCompat
import class Foundation.JSONEncoder
import class Foundation.JSONDecoder

protocol PSQLJSONEncoder {
    func encode<T: Encodable>(_ value: T, into buffer: inout ByteBuffer) throws
}

extension JSONEncoder: PSQLJSONEncoder {}
