import Atomics
import Foundation
import NIOCore
import Testing
import PostgresNIO

/// The tests must be serialized because the decoding test also uses the `_defaultJSONEncoder`,
/// increasing the counter in the encoding test.
///
/// ```swift
/// try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self)
/// ```
/// Here the `Object` is encoded first and then decoded.
@Suite(.serialized) struct PostgresJSONCodingTests {
    @Test(.bug("https://github.com/vapor/postgres-nio/issues/126"))
    func testCustomJSONEncoder() {
        let previousDefaultJSONEncoder = PostgresNIO._defaultJSONEncoder
        defer {
            PostgresNIO._defaultJSONEncoder = previousDefaultJSONEncoder
        }
        final class CustomJSONEncoder: PostgresJSONEncoder {
            let counter = ManagedAtomic(0)
            func encode<T>(_ value: T) throws -> Data where T : Encodable {
                self.counter.wrappingIncrement(ordering: .relaxed)
                return try JSONEncoder().encode(value)
            }
        }
        struct Object: Codable {
            var foo: Int
            var bar: Int
        }
        let customJSONEncoder = CustomJSONEncoder()
        #expect(customJSONEncoder.counter.load(ordering: .relaxed) == 0)
        PostgresNIO._defaultJSONEncoder = customJSONEncoder
        #expect(throws: Never.self) { try PostgresData(json: Object(foo: 1, bar: 2)) }
        #expect(customJSONEncoder.counter.load(ordering: .relaxed) == 1)

        let customJSONBEncoder = CustomJSONEncoder()
        #expect(customJSONBEncoder.counter.load(ordering: .relaxed) == 0)
        PostgresNIO._defaultJSONEncoder = customJSONBEncoder
        #expect(throws: Never.self) { try PostgresData(json: Object(foo: 1, bar: 2)) }
        #expect(customJSONBEncoder.counter.load(ordering: .relaxed) == 1)
    }

    @Test(.bug("https://github.com/vapor/postgres-nio/issues/126"))
    func testCustomJSONDecoder() {
        let previousDefaultJSONDecoder = PostgresNIO._defaultJSONDecoder
        defer {
            PostgresNIO._defaultJSONDecoder = previousDefaultJSONDecoder
        }
        final class CustomJSONDecoder: PostgresJSONDecoder {
            let counter = ManagedAtomic(0)
            func decode<T>(_ type: T.Type, from data: Data) throws -> T where T : Decodable {
                self.counter.wrappingIncrement(ordering: .relaxed)
                return try JSONDecoder().decode(type, from: data)
            }
        }
        struct Object: Codable {
            var foo: Int
            var bar: Int
        }
        let customJSONDecoder = CustomJSONDecoder()
        #expect(customJSONDecoder.counter.load(ordering: .relaxed) == 0)
        PostgresNIO._defaultJSONDecoder = customJSONDecoder
        #expect(throws: Never.self) { try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self) }
        #expect(customJSONDecoder.counter.load(ordering: .relaxed) == 1)

        let customJSONBDecoder = CustomJSONDecoder()
        #expect(customJSONBDecoder.counter.load(ordering: .relaxed) == 0)
        PostgresNIO._defaultJSONDecoder = customJSONBDecoder
        #expect(throws: Never.self) { try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self) }
        #expect(customJSONBDecoder.counter.load(ordering: .relaxed) == 1)
    }
}
