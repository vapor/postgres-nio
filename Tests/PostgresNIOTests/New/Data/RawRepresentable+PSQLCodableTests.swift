import Testing
import NIOCore
@testable import PostgresNIO

@Suite
struct RawRepresentable_PSQLCodableTests {
    enum MyRawRepresentable: Int16, PostgresCodable {
        case testing = 1
        case staging = 2
        case production = 3
    }

    @Test func roundTrip() throws {
        let values: [MyRawRepresentable] = [.testing, .staging, .production]

        for value in values {
            var buffer = ByteBuffer()
            try value.encode(into: &buffer, context: .default)
            #expect(MyRawRepresentable.psqlType == Int16.psqlType)
            #expect(buffer.readableBytes == 2)

            let result = try MyRawRepresentable(from: &buffer, type: Int16.psqlType, format: .binary, context: .default)
            #expect(value == result)
        }
    }

    @Test func decodeInvalidRawTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int16(4)) // out of bounds

        let error = #expect(throws: PostgresDecodingError.Code.self) {
            try MyRawRepresentable(from: &buffer, type: Int16.psqlType, format: .binary, context: .default)
        }
        #expect(error == .failure)
    }

    @Test func decodeInvalidUnderlyingTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // out of bounds

        let error = #expect(throws: PostgresDecodingError.Code.self) {
            try MyRawRepresentable(from: &buffer, type: Int32.psqlType, format: .binary, context: .default)
        }
        #expect(error == .failure)
    }
}
