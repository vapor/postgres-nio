import XCTest
import NIOCore
@testable import PostgresNIO

class UUID_PSQLCodableTests: XCTestCase {

    func testRoundTrip() {
        for _ in 0..<100 {
            let uuid = UUID()
            var buffer = ByteBuffer()

            uuid.encode(into: &buffer, context: .default)

            XCTAssertEqual(UUID.psqlType, .uuid)
            XCTAssertEqual(UUID.psqlFormat, .binary)
            XCTAssertEqual(buffer.readableBytes, 16)
            var byteIterator = buffer.readableBytesView.makeIterator()

            XCTAssertEqual(byteIterator.next(), uuid.uuid.0)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.1)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.2)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.3)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.4)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.5)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.6)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.7)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.8)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.9)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.10)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.11)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.12)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.13)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.14)
            XCTAssertEqual(byteIterator.next(), uuid.uuid.15)

            var decoded: UUID?
            XCTAssertNoThrow(decoded = try UUID(from: &buffer, type: .uuid, format: .binary, context: .default))
            XCTAssertEqual(decoded, uuid)
        }
    }

    func testDecodeFromString() {
        let options: [(PostgresFormat, PostgresDataType)] = [
            (.binary, .text),
            (.binary, .varchar),
            (.text, .uuid),
            (.text, .text),
            (.text, .varchar),
        ]

        for _ in 0..<100 {
            // use uppercase
            let uuid = UUID()
            var lowercaseBuffer = ByteBuffer()
            lowercaseBuffer.writeString(uuid.uuidString.lowercased())

            for (format, dataType) in options {
                var loopBuffer = lowercaseBuffer
                var decoded: UUID?
                XCTAssertNoThrow(decoded = try UUID(from: &loopBuffer, type: dataType, format: format, context: .default))
                XCTAssertEqual(decoded, uuid)
            }

            // use lowercase
            var uppercaseBuffer = ByteBuffer()
            uppercaseBuffer.writeString(uuid.uuidString)

            for (format, dataType) in options {
                var loopBuffer = uppercaseBuffer
                var decoded: UUID?
                XCTAssertNoThrow(decoded = try UUID(from: &loopBuffer, type: dataType, format: format, context: .default))
                XCTAssertEqual(decoded, uuid)
            }
        }
    }

    func testDecodeFailureFromBytes() {
        let uuid = UUID()
        var buffer = ByteBuffer()

        uuid.encode(into: &buffer, context: .default)
        // this makes only 15 bytes readable. this should lead to an error
        buffer.moveReaderIndex(forwardBy: 1)

        XCTAssertThrowsError(try UUID(from: &buffer, type: .uuid, format: .binary, context: .default)) { error in
            XCTAssertEqual(error as? PostgresDecodingError.Code, .failure)
        }
    }

    func testDecodeFailureFromString() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        buffer.writeString(uuid.uuidString)
        // this makes only 15 bytes readable. this should lead to an error
        buffer.moveReaderIndex(forwardBy: 1)

        let dataTypes: [PostgresDataType] = [.varchar, .text]

        for dataType in dataTypes {
            var loopBuffer = buffer
            XCTAssertThrowsError(try UUID(from: &loopBuffer, type: dataType, format: .binary, context: .default)) {
                XCTAssertEqual($0 as? PostgresDecodingError.Code, .failure)
            }
        }
    }

    func testDecodeFailureFromInvalidPostgresType() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        buffer.writeString(uuid.uuidString)

        let dataTypes: [PostgresDataType] = [.bool, .int8, .int2, .int4Array]

        for dataType in dataTypes {
            var copy = buffer
            XCTAssertThrowsError(try UUID(from: &copy, type: dataType, format: .binary, context: .default)) {
                XCTAssertEqual($0 as? PostgresDecodingError.Code, .typeMismatch)
            }
        }
    }
}
