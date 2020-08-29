import Foundation

public protocol PostgresJSONDecoder {
    func decode<T>(_ type: T.Type, from data: Data) throws -> T where T : Decodable
}

extension JSONDecoder: PostgresJSONDecoder {}

public var _defaultJSONDecoder: PostgresJSONDecoder = JSONDecoder()
