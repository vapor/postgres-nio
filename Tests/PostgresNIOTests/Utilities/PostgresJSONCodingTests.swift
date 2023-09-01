import Atomics
import NIOCore
import XCTest
import PostgresNIO

class PostgresJSONCodingTests: XCTestCase {
    // https://github.com/vapor/postgres-nio/issues/126
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
        XCTAssertEqual(customJSONEncoder.counter.load(ordering: .relaxed), 0)
        PostgresNIO._defaultJSONEncoder = customJSONEncoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)))
        XCTAssertEqual(customJSONEncoder.counter.load(ordering: .relaxed), 1)

        let customJSONBEncoder = CustomJSONEncoder()
        XCTAssertEqual(customJSONBEncoder.counter.load(ordering: .relaxed), 0)
        PostgresNIO._defaultJSONEncoder = customJSONBEncoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)))
        XCTAssertEqual(customJSONBEncoder.counter.load(ordering: .relaxed), 1)
    }

    // https://github.com/vapor/postgres-nio/issues/126
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
        XCTAssertEqual(customJSONDecoder.counter.load(ordering: .relaxed), 0)
        PostgresNIO._defaultJSONDecoder = customJSONDecoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self))
        XCTAssertEqual(customJSONDecoder.counter.load(ordering: .relaxed), 1)

        let customJSONBDecoder = CustomJSONDecoder()
        XCTAssertEqual(customJSONBDecoder.counter.load(ordering: .relaxed), 0)
        PostgresNIO._defaultJSONDecoder = customJSONBDecoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self))
        XCTAssertEqual(customJSONBDecoder.counter.load(ordering: .relaxed), 1)
    }
}
