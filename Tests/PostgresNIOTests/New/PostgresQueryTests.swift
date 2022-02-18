@testable import PostgresNIO
import XCTest

final class PostgresQueryTests: XCTestCase {

    func testStringInterpolation() throws {
        let string = "Hello World"
        let null: UUID? = nil
        let uuid: UUID? = UUID()

        let query: PSQLQuery = try """
            INSERT INTO foo (id, title, something) SET (\(uuid), \(string), \(null));
            """

        XCTAssertEqual(query.query, "INSERT INTO foo (id, title, something) SET ($1, $2, $3);")

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
}
