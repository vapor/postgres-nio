import Foundation

public protocol PostgresJSONEncoder {
    func encode<T>(_ value: T) throws -> Data where T : Encodable
}

extension JSONEncoder: PostgresJSONEncoder {}

public var _defaultJSONEncoder: PostgresJSONEncoder = JSONEncoder()
