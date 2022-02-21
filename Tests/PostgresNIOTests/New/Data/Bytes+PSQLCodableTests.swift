import XCTest
import NIOCore
@testable import PostgresNIO

class Bytes_PSQLCodableTests: XCTestCase {
    
    func testDataRoundTrip() {
        let data = Data((0...UInt8.max))
        
        var buffer = ByteBuffer()
        data.encode(into: &buffer, context: .default)
        XCTAssertEqual(data.psqlType, .bytea)
        
        var result: Data?
        XCTAssertNoThrow(result = try Data.decode(from: &buffer, type: .bytea, format: .binary, context: .default))
        XCTAssertEqual(data, result)
    }
    
    func testByteBufferRoundTrip() {
        let bytes = ByteBuffer(bytes: (0...UInt8.max))
        
        var buffer = ByteBuffer()
        bytes.encode(into: &buffer, context: .default)
        XCTAssertEqual(bytes.psqlType, .bytea)
        
        var result: ByteBuffer?
        XCTAssertNoThrow(result = try ByteBuffer.decode(from: &buffer, type: .bytea, format: .binary, context: .default))
        XCTAssertEqual(bytes, result)
    }
    
    func testEncodeSequenceWhereElementUInt8() {
        struct ByteSequence: Sequence, PSQLEncodable {
            typealias Element = UInt8
            typealias Iterator = Array<UInt8>.Iterator
            
            let bytes: [UInt8]
            
            init() {
                self.bytes = [UInt8]((0...UInt8.max))
            }
            
            func makeIterator() -> Array<UInt8>.Iterator {
                self.bytes.makeIterator()
            }
        }
        
        let sequence = ByteSequence()
        var buffer = ByteBuffer()
        sequence.encode(into: &buffer, context: .default)
        XCTAssertEqual(sequence.psqlType, .bytea)
        XCTAssertEqual(buffer.readableBytes, 256)
    }
}
