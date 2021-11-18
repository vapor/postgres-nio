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
            var didEncode = false
            func encode<T>(_ value: T) throws -> Data where T : Encodable {
                self.didEncode = true
                return try JSONEncoder().encode(value)
            }
        }
        struct Object: Codable {
            var foo: Int
            var bar: Int
        }
        let customJSONEncoder = CustomJSONEncoder()
        PostgresNIO._defaultJSONEncoder = customJSONEncoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)))
        XCTAssert(customJSONEncoder.didEncode)

        let customJSONBEncoder = CustomJSONEncoder()
        PostgresNIO._defaultJSONEncoder = customJSONBEncoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)))
        XCTAssert(customJSONBEncoder.didEncode)
    }

    // https://github.com/vapor/postgres-nio/issues/126
    func testCustomJSONDecoder() {
        let previousDefaultJSONDecoder = PostgresNIO._defaultJSONDecoder
        defer {
            PostgresNIO._defaultJSONDecoder = previousDefaultJSONDecoder
        }
        final class CustomJSONDecoder: PostgresJSONDecoder {
            var didDecode = false
            func decode<T>(_ type: T.Type, from data: Data) throws -> T where T : Decodable {
                self.didDecode = true
                return try JSONDecoder().decode(type, from: data)
            }
        }
        struct Object: Codable {
            var foo: Int
            var bar: Int
        }
        let customJSONDecoder = CustomJSONDecoder()
        PostgresNIO._defaultJSONDecoder = customJSONDecoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self))
        XCTAssert(customJSONDecoder.didDecode)

        let customJSONBDecoder = CustomJSONDecoder()
        PostgresNIO._defaultJSONDecoder = customJSONBDecoder
        XCTAssertNoThrow(try PostgresData(json: Object(foo: 1, bar: 2)).json(as: Object.self))
        XCTAssert(customJSONBDecoder.didDecode)
    }
}
