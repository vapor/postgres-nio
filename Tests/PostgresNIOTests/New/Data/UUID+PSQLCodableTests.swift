import struct Foundation.UUID
import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct UUID_PSQLCodableTests {
    @Test func testRoundTrip() {
        for _ in 0..<100 {
            let uuid = UUID()
            var buffer = ByteBuffer()

            uuid.encode(into: &buffer, context: .default)

            #expect(UUID.psqlType == .uuid)
            #expect(UUID.psqlFormat == .binary)
            #expect(buffer.readableBytes == 16)
            var byteIterator = buffer.readableBytesView.makeIterator()

            #expect(byteIterator.next() == uuid.uuid.0)
            #expect(byteIterator.next() == uuid.uuid.1)
            #expect(byteIterator.next() == uuid.uuid.2)
            #expect(byteIterator.next() == uuid.uuid.3)
            #expect(byteIterator.next() == uuid.uuid.4)
            #expect(byteIterator.next() == uuid.uuid.5)
            #expect(byteIterator.next() == uuid.uuid.6)
            #expect(byteIterator.next() == uuid.uuid.7)
            #expect(byteIterator.next() == uuid.uuid.8)
            #expect(byteIterator.next() == uuid.uuid.9)
            #expect(byteIterator.next() == uuid.uuid.10)
            #expect(byteIterator.next() == uuid.uuid.11)
            #expect(byteIterator.next() == uuid.uuid.12)
            #expect(byteIterator.next() == uuid.uuid.13)
            #expect(byteIterator.next() == uuid.uuid.14)
            #expect(byteIterator.next() == uuid.uuid.15)

            var decoded: UUID?
            #expect(throws: Never.self) {
                decoded = try UUID(from: &buffer, type: .uuid, format: .binary, context: .default)
            }
            #expect(decoded == uuid)
        }
    }

    @Test func testDecodeFromString() {
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
                #expect(throws: Never.self) {
                    decoded = try UUID(from: &loopBuffer, type: dataType, format: format, context: .default)
                }
                #expect(decoded == uuid)
            }

            // use lowercase
            var uppercaseBuffer = ByteBuffer()
            uppercaseBuffer.writeString(uuid.uuidString)

            for (format, dataType) in options {
                var loopBuffer = uppercaseBuffer
                var decoded: UUID?
                #expect(throws: Never.self) {
                    decoded = try UUID(from: &loopBuffer, type: dataType, format: format, context: .default)
                }
                #expect(decoded == uuid)

            }
        }
    }

    @Test func testDecodeFailureFromBytes() {
        let uuid = UUID()
        var buffer = ByteBuffer()

        uuid.encode(into: &buffer, context: .default)
        // this makes only 15 bytes readable. this should lead to an error
        buffer.moveReaderIndex(forwardBy: 1)

        let error = #expect(throws: PostgresDecodingError.Code.self) {
            try UUID(from: &buffer, type: .uuid, format: .binary, context: .default)
        }
        #expect(error == .failure)
    }

    @Test func testDecodeFailureFromString() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        buffer.writeString(uuid.uuidString)
        // this makes only 15 bytes readable. this should lead to an error
        buffer.moveReaderIndex(forwardBy: 1)

        let dataTypes: [PostgresDataType] = [.varchar, .text]

        for dataType in dataTypes {
            var loopBuffer = buffer
            let error = #expect(throws: PostgresDecodingError.Code.self) {
                try UUID(from: &loopBuffer, type: dataType, format: .binary, context: .default)
            }
            #expect(error == .failure)
        }
    }

    @Test func testDecodeFailureFromInvalidPostgresType() {
        let uuid = UUID()
        var buffer = ByteBuffer()
        buffer.writeString(uuid.uuidString)

        let dataTypes: [PostgresDataType] = [.bool, .int8, .int2, .int4Array]

        for dataType in dataTypes {
            var copy = buffer
            let error = #expect(throws: PostgresDecodingError.Code.self) {
                try UUID(from: &copy, type: dataType, format: .binary, context: .default)
            }
            #expect(error == .typeMismatch)
        }
    }
}
