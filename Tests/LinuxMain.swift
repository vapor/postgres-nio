import XCTest

import nio_postgresTests

var tests = [XCTestCaseEntry]()
tests += nio_postgresTests.allTests()
XCTMain(tests)