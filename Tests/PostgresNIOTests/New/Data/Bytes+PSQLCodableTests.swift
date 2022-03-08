import XCTest
import NIOCore
@testable import PostgresNIO

class Bytes_PSQLCodableTests: XCTestCase {
    
    func testDataRoundTrip() {
        let data = Data((0...UInt8.max))
        
        var buffer = ByteBuffer()
        data.encode(into: &buffer, context: .default)
        XCTAssertEqual(ByteBuffer.psqlType, .bytea)
        
        var result: Data?
        result = Data(from: &buffer, type: .bytea, format: .binary, context: .default)
        XCTAssertEqual(data, result)
    }
    
    func testByteBufferRoundTrip() {
        let bytes = ByteBuffer(bytes: (0...UInt8.max))
        
        var buffer = ByteBuffer()
        bytes.encode(into: &buffer, context: .default)
        XCTAssertEqual(ByteBuffer.psqlType, .bytea)
        
        var result: ByteBuffer?
        result = ByteBuffer(from: &buffer, type: .bytea, format: .binary, context: .default)
        XCTAssertEqual(bytes, result)
    }
    
    func testEncodeSequenceWhereElementUInt8() {
        struct ByteSequence: Sequence, PostgresEncodable {
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
        XCTAssertEqual(ByteSequence.psqlType, .bytea)
        XCTAssertEqual(buffer.readableBytes, 256)
    }
}
