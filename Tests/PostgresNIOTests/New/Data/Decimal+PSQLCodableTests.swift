import Testing
import NIOCore
@testable import PostgresNIO

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

@Suite
struct Decimal_PSQLCodableTests {
    @Test func roundTrip() throws {
        let values: [Decimal] = [1.1, .pi, -5e-12, 0.00000080216390553684]

        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .default)
            #expect(Decimal.psqlType == .numeric)

            let result = try Decimal(from: &buffer, type: .numeric, format: .binary, context: .default)
            #expect(value == result)
        }
    }

    @Test func decodeFailureInvalidType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))

        #expect(throws: PostgresDecodingError.Code.typeMismatch) {
            try Decimal(from: &buffer, type: .int8, format: .binary, context: .default)
        }
    }

}
