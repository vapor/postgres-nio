@testable import PostgresNIO
import XCTest
import NIOCore

final class PostgresQueryTests: XCTestCase {

    func testStringInterpolationWithOptional() {
        let string = "Hello World"
        let null: UUID? = nil
        let uuid: UUID? = UUID()

        let query: PostgresQuery = """
            INSERT INTO foo (id, title, something) SET (\(uuid), \(string), \(null));
            """

        XCTAssertEqual(query.sql, "INSERT INTO foo (id, title, something) SET ($1, $2, $3);")

        var expected = ByteBuffer()
        expected.writeInteger(Int32(16))
        expected.writeBytes([
            uuid!.uuid.0, uuid!.uuid.1, uuid!.uuid.2, uuid!.uuid.3,
            uuid!.uuid.4, uuid!.uuid.5, uuid!.uuid.6, uuid!.uuid.7,
            uuid!.uuid.8, uuid!.uuid.9, uuid!.uuid.10, uuid!.uuid.11,
            uuid!.uuid.12, uuid!.uuid.13, uuid!.uuid.14, uuid!.uuid.15,
        ])

        expected.writeInteger(Int32(string.utf8.count))
        expected.writeString(string)
        expected.writeInteger(Int32(-1))

        XCTAssertEqual(query.binds.bytes, expected)
    }

    func testStringInterpolationWithDynamicType() {
        let type = PostgresDataType(16435)
        let format = PostgresFormat.binary
        let dynamicString = DynamicString(value: "Hello world", psqlType: type, psqlFormat: format)

        let query: PostgresQuery = """
        INSERT INTO foo (dynamicType) SET (\(dynamicString));
        """

        XCTAssertEqual(query.sql, "INSERT INTO foo (dynamicType) SET ($1);")

        var expectedBindsBytes = ByteBuffer()
        expectedBindsBytes.writeInteger(Int32(dynamicString.value.utf8.count))
        expectedBindsBytes.writeString(dynamicString.value)

        let expectedMetadata: [PostgresBindings.Metadata] = [.init(dataType: type, format: format, protected: true)]

        XCTAssertEqual(query.binds.bytes, expectedBindsBytes)
        XCTAssertEqual(query.binds.metadata, expectedMetadata)
    }

    func testStringInterpolationWithCustomJSONEncoder() {
        struct Foo: Codable, PostgresCodable {
            var helloWorld: String
        }

        let jsonEncoder = JSONEncoder()
        jsonEncoder.keyEncodingStrategy = .convertToSnakeCase

        var query: PostgresQuery?
        XCTAssertNoThrow(query = try """
            INSERT INTO test (foo) SET (\(Foo(helloWorld: "bar"), context: .init(jsonEncoder: jsonEncoder)));
            """
        )

        XCTAssertEqual(query?.sql, "INSERT INTO test (foo) SET ($1);")

        let expectedJSON = #"{"hello_world":"bar"}"#

        var expected = ByteBuffer()
        expected.writeInteger(Int32(expectedJSON.utf8.count + 1))
        expected.writeInteger(UInt8(0x01))
        expected.writeString(expectedJSON)

        XCTAssertEqual(query?.binds.bytes, expected)
    }

    func testAllowUsersToGenerateLotsOfRows() {
        let sql = "INSERT INTO test (id) SET (\((1...5).map({"$\($0)"}).joined(separator: ", ")));"

        var query = PostgresQuery(unsafeSQL: sql, binds: .init(capacity: 5))
        for value in 1...5 {
            query.binds.append(Int(value), context: .default)
        }

        XCTAssertEqual(query.sql, "INSERT INTO test (id) SET ($1, $2, $3, $4, $5);")

        var expected = ByteBuffer()
        for value in 1...5 {
            expected.writeInteger(UInt32(8))
            expected.writeInteger(value)
        }

        XCTAssertEqual(query.binds.bytes, expected)
    }

    func testStringInterpolationWithSequence() throws {
        let titles = ["bar", "baz"]
        let query: PostgresQuery = try """
        SELECT * FROM foo WHERE title in \(titles)
        """

        XCTAssertEqual(query.sql, "SELECT * FROM foo WHERE title in ($1, $2)")

        var expected = ByteBuffer()
        for title in titles {
            expected.writeInteger(UInt32(3))
            expected.writeString(title)
        }

        XCTAssertEqual(query.binds.bytes, expected)
    }

    func testUnescapedSQL() {
        let tableName = UUID().uuidString.uppercased()
        let value = 1

        let query: PostgresQuery = "INSERT INTO \(unescaped: tableName) (id) SET (\(value));"

        var expected = ByteBuffer()
        expected.writeInteger(UInt32(8))
        expected.writeInteger(value)

        XCTAssertEqual(query.binds.bytes, expected)
    }
}

extension PostgresQueryTests {
    struct DynamicString: PostgresDynamicTypeEncodable {
        let value: String

        var psqlType: PostgresDataType
        var psqlFormat: PostgresFormat

        func encode<JSONEncoder>(
            into byteBuffer: inout ByteBuffer,
            context: PostgresNIO.PostgresEncodingContext<JSONEncoder>
        ) where JSONEncoder: PostgresJSONEncoder {
            byteBuffer.writeString(value)
        }
    }
}
