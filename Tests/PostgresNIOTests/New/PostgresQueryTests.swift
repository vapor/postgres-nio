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

    func testQueryInterpolationBeforeParentBinds() {
        let a = 1
        let b = 2
        let subQuery: PostgresQuery = "SELECT id FROM sub WHERE a = \(a)"

        let query: PostgresQuery = "SELECT (\(subQuery)) FROM table WHERE b = \(b)"

        XCTAssertEqual(query.sql, "SELECT (SELECT id FROM sub WHERE a = $1) FROM table WHERE b = $2")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([a, b]))
    }

    func testQueryInterpolationAfterParentBind() {
        let a = 1
        let b = 2
        let subQuery: PostgresQuery = "SELECT id FROM sub WHERE a = \(a)"

        let query: PostgresQuery = "SELECT * FROM table WHERE b = \(b) AND id IN (\(subQuery))"

        XCTAssertEqual(query.sql, "SELECT * FROM table WHERE b = $1 AND id IN (SELECT id FROM sub WHERE a = $2)")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([b, a]))
    }

    func testQueryInterpolationBetweenParentBinds() {
        let a = 10
        let b = 20
        let c = 1
        let d = 2
        let fragment: PostgresQuery = "a = \(a) AND b = \(b)"

        let query: PostgresQuery = "SELECT * FROM t WHERE c = \(c) AND (\(fragment)) AND d = \(d)"

        XCTAssertEqual(query.sql, "SELECT * FROM t WHERE c = $1 AND (a = $2 AND b = $3) AND d = $4")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([c, a, b, d]))
    }

    func testQueryInterpolationWithEmptyQuery() {
        let condition: PostgresQuery = ""

        let query: PostgresQuery = "SELECT * FROM t \(condition)"

        XCTAssertEqual(query.sql, "SELECT * FROM t ")
        XCTAssertEqual(query.binds.count, 0)
    }

    func testQueryInterpolationWithBindlessQuery() {
        let condition: PostgresQuery = "WHERE deleted_at IS NULL"

        let query: PostgresQuery = "SELECT * FROM t \(condition)"

        XCTAssertEqual(query.sql, "SELECT * FROM t WHERE deleted_at IS NULL")
        XCTAssertEqual(query.binds.count, 0)
    }

    func testNestedQueryInterpolation() {
        let x = 1
        let y = 2
        let z = 3
        let inner: PostgresQuery = "x = \(x)"
        let middle: PostgresQuery = "y = \(y) AND (\(inner))"

        let query: PostgresQuery = "SELECT * FROM t WHERE z = \(z) AND (\(middle))"

        XCTAssertEqual(query.sql, "SELECT * FROM t WHERE z = $1 AND (y = $2 AND (x = $3))")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([z, y, x]))
    }

    func testQueryInterpolatedTwice() {
        let a = 1
        let fragment: PostgresQuery = "a = \(a)"

        let query: PostgresQuery = "SELECT * FROM t WHERE \(fragment) OR \(fragment)"

        XCTAssertEqual(query.sql, "SELECT * FROM t WHERE a = $1 OR a = $2")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([a, a]))
    }

    func testQueryInterpolationRenumbersMultiDigitPlaceholders() {
        let ids = Array(1...12)
        let a = 0
        let fragment: PostgresQuery = "ids IN (\(ids[0]), \(ids[1]), \(ids[2]), \(ids[3]), \(ids[4]), \(ids[5]), \(ids[6]), \(ids[7]), \(ids[8]), \(ids[9]), \(ids[10]), \(ids[11]))"
        XCTAssertEqual(fragment.sql, "ids IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)")

        let query: PostgresQuery = "SELECT * FROM t WHERE a = \(a) AND \(fragment)"

        XCTAssertEqual(query.sql, "SELECT * FROM t WHERE a = $1 AND ids IN ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([a] + ids))
    }

    func testQueryInterpolationLeavesQuotedPlaceholderTextAlone() {
        let a = 1
        let b = 2
        let fragment: PostgresQuery = "note = 'costs $1' AND a = \(a)"
        XCTAssertEqual(fragment.sql, "note = 'costs $1' AND a = $1")

        let query: PostgresQuery = "SELECT * FROM t WHERE b = \(b) AND \(fragment)"

        XCTAssertEqual(query.sql, "SELECT * FROM t WHERE b = $1 AND note = 'costs $1' AND a = $2")
        XCTAssertEqual(query.binds.bytes, Self.intBinds([b, a]))
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
    private static func intBinds(_ values: [Int]) -> ByteBuffer {
        var buffer = ByteBuffer()
        for value in values {
            buffer.writeInteger(UInt32(8))
            buffer.writeInteger(value)
        }
        return buffer
    }

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
