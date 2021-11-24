import XCTest
import NIOCore
@testable import PostgresNIO

class Decimal_PSQLCodableTests: XCTestCase {
    
    func testRoundTrip() {
        let values: [Decimal] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .forTests())
            XCTAssertEqual(value.psqlType, .numeric)
            let data = PSQLData(bytes: buffer, dataType: .numeric, format: .binary)
            
            var result: Decimal?
            XCTAssertNoThrow(result = try data.decode(as: Decimal.self, context: .forTests()))
            XCTAssertEqual(value, result)
        }
    }
    
    func testDecodeFailureInvalidType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))
        let data = PSQLData(bytes: buffer, dataType: .int8, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Decimal.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
}
