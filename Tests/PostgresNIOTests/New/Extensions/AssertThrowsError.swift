import XCTest

/// Same as `XCTAssertThrows` but allows the expression to be async
func assertThrowsError<T>(
  _ expression: @autoclosure () async throws -> T,
  _ message: @autoclosure () -> String = "",
  file: StaticString = #filePath,
  line: UInt = #line,
  errorHandler: (_ error: Error) -> Void = { _ in }
) async {
  do {
    _ = try await expression()
    XCTFail("Expression was expected to throw but did not throw", file: file, line: line)
  } catch {
    errorHandler(error)
  }
}
