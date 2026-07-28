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

    func testStringInterpolationWithGroup() throws {
        let ids = [1, 2, 3]

        let query: PostgresQuery = "SELECT * FROM foo WHERE id IN (\(group: ids));"

        XCTAssertEqual(query.sql, "SELECT * FROM foo WHERE id IN ($1, $2, $3);")
        XCTAssertEqual(query.binds.count, 3)

        var expected = ByteBuffer()
        for id in ids {
            expected.writeInteger(UInt32(8))
            expected.writeInteger(id)
        }

        XCTAssertEqual(query.binds.bytes, expected)
        XCTAssertEqual(
            query.binds.metadata,
            ids.map { _ in .init(dataType: .int8, format: .binary, protected: true) }
        )
    }

    func testStringInterpolationWithGroupContinuesBindNumbering() throws {
        let title = "Hello World"
        let ids = [1, 2]
        let limit = 10

        let query: PostgresQuery = """
            SELECT * FROM foo WHERE title = \(title) AND id IN (\(group: ids)) LIMIT \(limit);
            """

        XCTAssertEqual(query.sql, "SELECT * FROM foo WHERE title = $1 AND id IN ($2, $3) LIMIT $4;")
        XCTAssertEqual(query.binds.count, 4)
    }

    func testStringInterpolationWithSingleElementGroup() throws {
        let query: PostgresQuery = "SELECT * FROM foo WHERE id IN (\(group: [1]));"

        XCTAssertEqual(query.sql, "SELECT * FROM foo WHERE id IN ($1);")
        XCTAssertEqual(query.binds.count, 1)
    }

    func testStringInterpolationWithEmptyGroup() throws {
        let ids: [Int] = []

        let query: PostgresQuery = "SELECT * FROM foo WHERE id IN (\(group: ids));"

        XCTAssertEqual(query.sql, "SELECT * FROM foo WHERE id IN ();")
        XCTAssertEqual(query.binds.count, 0)
    }

    func testStringInterpolationWithGroupOfDynamicType() throws {
        let type = PostgresDataType(16435)
        let format = PostgresFormat.binary
        let values = ["foo", "bar"].map { DynamicString(value: $0, psqlType: type, psqlFormat: format) }

        let query: PostgresQuery = "SELECT * FROM foo WHERE bar IN (\(group: values));"

        XCTAssertEqual(query.sql, "SELECT * FROM foo WHERE bar IN ($1, $2);")

        var expected = ByteBuffer()
        for value in values {
            expected.writeInteger(Int32(value.value.utf8.count))
            expected.writeString(value.value)
        }

        XCTAssertEqual(query.binds.bytes, expected)
        XCTAssertEqual(
            query.binds.metadata,
            values.map { _ in .init(dataType: type, format: format, protected: true) }
        )
    }

    func testStringInterpolationWithThrowingGroupRethrows() {
        let values = [
            ThrowingDynamicString(value: "foo"),
            ThrowingDynamicString(value: ""),
        ]

        var query: PostgresQuery?
        XCTAssertThrowsError(query = try "SELECT * FROM foo WHERE bar IN (\(group: values));") {
            XCTAssert($0 is ThrowingDynamicString.EncodingError)
        }
        XCTAssertNil(query)
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

    struct ThrowingDynamicString: PostgresThrowingDynamicTypeEncodable {
        struct EncodingError: Error {}

        let value: String

        var psqlType: PostgresDataType { .text }
        var psqlFormat: PostgresFormat { .binary }

        func encode<JSONEncoder>(
            into byteBuffer: inout ByteBuffer,
            context: PostgresNIO.PostgresEncodingContext<JSONEncoder>
        ) throws where JSONEncoder: PostgresJSONEncoder {
            guard !self.value.isEmpty else { throw EncodingError() }
            byteBuffer.writeString(self.value)
        }
    }
}
