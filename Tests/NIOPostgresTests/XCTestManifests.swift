import XCTest

extension NIOPostgresTests {
    static let __allTests = [
        ("testConnectAndClose", testConnectAndClose),
        ("testSimpleQueryVersion", testSimpleQueryVersion),
        ("testQueryVersion", testQueryVersion),
        ("testQuerySelectParameter", testQuerySelectParameter),
        ("testSQLError", testSQLError),
        ("testSelectTypes", testSelectTypes),
        ("testSelectType", testSelectType),
        ("testIntegers", testIntegers),
        ("testPi", testPi),
        ("testUUID", testUUID),
        ("testSelectPerformance", testSelectPerformance),
        ("testRangeSelectPerformance", testRangeSelectPerformance),
        ("testRangeSelectDecodePerformance", testRangeSelectDecodePerformance),
    ]
}

#if !os(macOS)
public func __allTests() -> [XCTestCaseEntry] {
    return [
        testCase(NIOPostgresTests.__allTests),
    ]
}
#endif
