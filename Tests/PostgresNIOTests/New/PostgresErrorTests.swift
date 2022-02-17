@testable import PostgresNIO
import XCTest

final class PostgresCastingErrorTests: XCTestCase {
    func testPostgresCastingErrorEquality() {
        let error1 = PostgresCastingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresData: ByteBuffer(string: "hello world")
        )

        let error2 = PostgresCastingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: Int.self,
            postgresType: .text,
            postgresData: ByteBuffer(string: "hello world")
        )

        XCTAssertNotEqual(error1, error2)
        let error3 = error1
        XCTAssertEqual(error1, error3)
    }
}
