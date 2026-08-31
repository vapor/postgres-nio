@testable import PostgresNIO
import Testing
import NIOCore
import Foundation

@Suite struct PostgresQueryTests {
    @Test func stringInterpolationWithOptional() {
        let string = "Hello World"
        let null: UUID? = nil
        let uuid: UUID? = UUID()

        let query: PostgresQuery = """
            INSERT INTO foo (id, title, something) SET (\(uuid), \(string), \(null));
            """

        #expect(query.sql == "INSERT INTO foo (id, title, something) SET ($1, $2, $3);")

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

        #expect(query.binds.bytes == expected)
    }

    @Test func stringInterpolationWithDynamicType() {
        let type = PostgresDataType(16435)
        let format = PostgresFormat.binary
        let dynamicString = DynamicString(value: "Hello world", psqlType: type, psqlFormat: format)

        let query: PostgresQuery = """
        INSERT INTO foo (dynamicType) SET (\(dynamicString));
        """

        #expect(query.sql == "INSERT INTO foo (dynamicType) SET ($1);")

        var expectedBindsBytes = ByteBuffer()
        expectedBindsBytes.writeInteger(Int32(dynamicString.value.utf8.count))
        expectedBindsBytes.writeString(dynamicString.value)

        let expectedMetadata: [PostgresBindings.Metadata] = [.init(dataType: type, format: format, protected: true)]

        #expect(query.binds.bytes == expectedBindsBytes)
        #expect(query.binds.metadata == expectedMetadata)
    }

    @Test func stringInterpolationWithCustomJSONEncoder() {
        struct Foo: Codable, PostgresCodable {
            var helloWorld: String
        }

        let jsonEncoder = JSONEncoder()
        jsonEncoder.keyEncodingStrategy = .convertToSnakeCase

        var query: PostgresQuery?
        #expect(throws: Never.self) { 
            query = try """
                INSERT INTO test (foo) SET (\(Foo(helloWorld: "bar"), context: .init(jsonEncoder: jsonEncoder)));
                """
        }

        #expect(query?.sql == "INSERT INTO test (foo) SET ($1);")

        let expectedJSON = #"{"hello_world":"bar"}"#

        var expected = ByteBuffer()
        expected.writeInteger(Int32(expectedJSON.utf8.count + 1))
        expected.writeInteger(UInt8(0x01))
        expected.writeString(expectedJSON)

        #expect(query?.binds.bytes == expected)
    }

    @Test func allowUsersToGenerateLotsOfRows() {
        let sql = "INSERT INTO test (id) SET (\((1...5).map({"$\($0)"}).joined(separator: ", ")));"

        var query = PostgresQuery(unsafeSQL: sql, binds: .init(capacity: 5))
        for value in 1...5 {
            query.binds.append(Int(value), context: .default)
        }

        #expect(query.sql == "INSERT INTO test (id) SET ($1, $2, $3, $4, $5);")

        var expected = ByteBuffer()
        for value in 1...5 {
            expected.writeInteger(UInt32(8))
            expected.writeInteger(value)
        }

        #expect(query.binds.bytes == expected)
    }

    @Test func unescapedSQL() {
        let tableName = UUID().uuidString.uppercased()
        let value = 1

        let query: PostgresQuery = "INSERT INTO \(unescaped: tableName) (id) SET (\(value));"

        var expected = ByteBuffer()
        expected.writeInteger(UInt32(8))
        expected.writeInteger(value)

        #expect(query.binds.bytes == expected)
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
