import XCTest

extension NIOPostgresTests {
    static let __allTests = [
        ("testConnectAndClose", testConnectAndClose),
        ("testSimpleQueryVersion", testSimpleQueryVersion),
    ]
}

#if !os(macOS)
public func __allTests() -> [XCTestCaseEntry] {
    return [
        testCase(NIOPostgresTests.__allTests),
    ]
}
#endif
