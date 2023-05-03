@testable import PostgresNIO
import XCTest
import NIOCore

final class PostgresDecodingErrorTests: XCTestCase {
    func testPostgresDecodingErrorEquality() {
        let error1 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        let error2 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: Int.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        XCTAssertNotEqual(error1, error2)
        let error3 = error1
        XCTAssertEqual(error1, error3)
    }

    func testPostgresDecodingErrorDescription() {
        let error1 = PostgresDecodingError(
            code: .typeMismatch,
            columnName: "column",
            columnIndex: 0,
            targetType: String.self,
            postgresType: .text,
            postgresFormat: .binary,
            postgresData: ByteBuffer(string: "hello world"),
            file: "foo.swift",
            line: 123
        )

        let error2 = PostgresDecodingError(
            code: .missingData,
            columnName: "column",
            columnIndex: 0,
            targetType: [[String: String]].self,
            postgresType: .jsonbArray,
            postgresFormat: .binary,
            postgresData: nil,
            file: "bar.swift",
            line: 123
        )

        // Plain description
        XCTAssertEqual(String(describing: error1), "Database error")
        XCTAssertEqual(String(describing: error2), "Database error")
        
        // Extended debugDescription
        XCTAssertEqual(String(reflecting: error1), """
            PostgresDecodingError(code: typeMismatch,\
             columnName: "column", columnIndex: 0,\
             targetType: Swift.String,\
             postgresType: TEXT, postgresFormat: binary,\
             postgresData: \(error1.postgresData?.debugDescription ?? "nil"),\
             file: foo.swift, line: 123\
            )
            """)
        XCTAssertEqual(String(reflecting: error2), """
            PostgresDecodingError(code: missingData,\
             columnName: "column", columnIndex: 0,\
             targetType: Swift.Array<Swift.Dictionary<Swift.String, Swift.String>>,\
             postgresType: JSONB[], postgresFormat: binary,\
             file: bar.swift, line: 123\
            )
            """)
    }
}
