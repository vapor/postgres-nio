import XCTest

#if !os(macOS)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(nio_postgresTests.allTests),
    ]
}
#endif