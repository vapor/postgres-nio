import XCTest
import NIOCore
@testable import PostgresNIO

class Date_PSQLCodableTests: XCTestCase {

    func testNowRoundTrip() {
        let value = Date()

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        XCTAssertEqual(Date.psqlType, .timestamptz)
        XCTAssertEqual(buffer.readableBytes, 8)

        var result: Date?
        XCTAssertNoThrow(result = try Date(from: &buffer, type: .timestamptz, format: .binary, context: .default))
        XCTAssertEqual(value.timeIntervalSince1970, result?.timeIntervalSince1970 ?? 0, accuracy: 0.001)
    }

    func testDecodeRandomDate() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        var result: Date?
        XCTAssertNoThrow(result = try Date(from: &buffer, type: .timestamptz, format: .binary, context: .default))
        XCTAssertNotNil(result)
    }

    func testDecodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        XCTAssertThrowsError(try Date(from: &buffer, type: .timestamptz, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecodingError.Code, .failure)
        }
    }

    func testDecodeDate() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)

        var firstDate: Date?
        XCTAssertNoThrow(firstDate = try Date(from: &firstDateBuffer, type: .date, format: .binary, context: .default))
        XCTAssertNotNil(firstDate)

        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)

        var lastDate: Date?
        XCTAssertNoThrow(lastDate = try Date(from: &lastDateBuffer, type: .date, format: .binary, context: .default))
        XCTAssertNotNil(lastDate)
    }

    func testDecodeDateFromTimestamp() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)

        var firstDate: Date?
        XCTAssertNoThrow(firstDate = try Date(from: &firstDateBuffer, type: .date, format: .binary, context: .default))
        XCTAssertNotNil(firstDate)

        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)

        var lastDate: Date?
        XCTAssertNoThrow(lastDate = try Date(from: &lastDateBuffer, type: .date, format: .binary, context: .default))
        XCTAssertNotNil(lastDate)
    }

    func testDecodeDateFailsWithTooMuchData() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        XCTAssertThrowsError(try Date(from: &buffer, type: .date, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecodingError.Code, .failure)
        }
    }

    func testDecodeDateFailsWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        XCTAssertThrowsError(try Date(from: &buffer, type: .int8, format: .binary, context: .default)) {
            XCTAssertEqual($0 as? PostgresDecodingError.Code, .typeMismatch)
        }
    }

}
