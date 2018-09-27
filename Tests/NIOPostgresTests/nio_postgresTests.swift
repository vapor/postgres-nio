import XCTest
@testable import nio_postgres

final class nio_postgresTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(nio_postgres().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
