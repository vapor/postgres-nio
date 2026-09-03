import Foundation
import NIOCore
import Testing

@testable import PostgresNIO

@Suite
struct Date_PSQLCodableTests {

    @Test func nowRoundTrip() throws {
        let value = Date()

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(Date.psqlType == .timestamptz)
        #expect(buffer.readableBytes == 8)

        let result = try Date(from: &buffer, type: .timestamptz, format: .binary, context: .default)
        #expect(abs(value.timeIntervalSince1970 - result.timeIntervalSince1970) < 0.001)
    }

    @Test func decodeRandomDate() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        var result: Date?
        #expect(throws: Never.self) { 
            result = try Date(from: &buffer, type: .timestamptz, format: .binary, context: .default) 
        }
        #expect(result != nil)
    }

    @Test func decodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))

        #expect(throws: PostgresDecodingError.Code.failure) { 
            try Date(from: &buffer, type: .timestamptz, format: .binary, context: .default) 
        }
    }

    @Test func decodeDate() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)

        var firstDate: Date?
        #expect(throws: Never.self) { 
            firstDate = try Date(from: &firstDateBuffer, type: .date, format: .binary, context: .default) 
        }
        #expect(firstDate != nil)

        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)

        var lastDate: Date?
        #expect(throws: Never.self) { 
            lastDate = try Date(from: &lastDateBuffer, type: .date, format: .binary, context: .default) 
        }
        #expect(lastDate != nil)
    }

    @Test func decodeDateFailsWithTooMuchData() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        #expect(throws: PostgresDecodingError.Code.failure) {
            try Date(from: &buffer, type: .date, format: .binary, context: .default)
        }
    }

    @Test func decodeDateFailsWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        #expect(throws: PostgresDecodingError.Code.typeMismatch) {
            try Date(from: &buffer, type: .int8, format: .binary, context: .default)
        }
    }

    @Test(
        .bug("https://github.com/vapor/postgres-nio/issues/632"),
        arguments: [
            (Date(timeIntervalSince1970: .greatestFiniteMagnitude), Date._endTimestamp),
            (Date(timeIntervalSince1970: -.greatestFiniteMagnitude), Date._minTimestamp - 1),
            (Date(timeIntervalSince1970: .infinity), Date._endTimestamp),
            (Date(timeIntervalSince1970: -.infinity), Date._minTimestamp - 1),
            (Date(timeIntervalSince1970: .nan), Date._endTimestamp),
            (Date(timeInterval: 9_300_000_000_000, since: Date(timeIntervalSince1970: 946_684_800)), Date._endTimestamp),
            (Date(timeInterval: -9_300_000_000_000, since: Date(timeIntervalSince1970: 946_684_800)), Date._minTimestamp - 1)
        ])
    func encodeDatesOutsideOfInt64MicrosecondRange(value: Date, expected: Int64) {
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(buffer.readInteger(as: Int64.self) == expected)
    }

    @Test(.bug("https://github.com/vapor/postgres-nio/issues/632"))
    func encodeDateLandingExactlyOnNegativeInfinity() {
        let value = Date(
            timeInterval: -9_223_372_036_854.775390625,
            since: Date(timeIntervalSince1970: 946_684_800)
        )
        // Precondition for this test to be meaningful: the naive conversion does produce `Int64.min`.
        let naive = value.timeIntervalSince(Date(timeIntervalSince1970: 946_684_800)) * 1_000_000
        #expect(Int64(exactly: naive.rounded(.towardZero)) == Int64.min)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(buffer.readInteger(as: Int64.self) == Date._minTimestamp - 1)
    }
}
