import Foundation

/// A protocol that mimmicks the Foundation `JSONDecoder.decode(_:from:)` function.
/// Conform a non-Foundation JSON decoder to this protocol if you want PostgresNIO to be
/// able to use it when decoding JSON & JSONB values (see `PostgresNIO._defaultJSONDecoder`)
public protocol PostgresJSONDecoder {
    func decode<T>(_ type: T.Type, from data: Data) throws -> T where T : Decodable
}

extension JSONDecoder: PostgresJSONDecoder {}

/// The default JSON decoder used by PostgresNIO when decoding JSON & JSONB values.
/// As `_defaultJSONDecoder` will be reused for decoding all JSON & JSONB values
/// from potentially multiple threads at once, you must ensure your custom JSON decoder is
/// thread safe internally like `Foundation.JSONDecoder`.
public var _defaultJSONDecoder: PostgresJSONDecoder = JSONDecoder()
