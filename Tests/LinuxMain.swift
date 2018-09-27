import XCTest

import NIOPostgresTests

var tests = [XCTestCaseEntry]()
tests += NIOPostgresTests.__allTests()

XCTMain(tests)
