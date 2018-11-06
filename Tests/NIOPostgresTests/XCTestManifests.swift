import XCTest

extension NIOPostgresTests {
    static let __allTests = [
        ("testConnectAndClose", testConnectAndClose),
        ("testSimpleQueryVersion", testSimpleQueryVersion),
        ("testQueryVersion", testQueryVersion),
        ("testQuerySelectParameter", testQuerySelectParameter),
        ("testSQLError", testSQLError),
        ("testSelectTypes", testSelectTypes),
    ]
}

#if !os(macOS)
public func __allTests() -> [XCTestCaseEntry] {
    return [
        testCase(NIOPostgresTests.__allTests),
    ]
}
#endif
