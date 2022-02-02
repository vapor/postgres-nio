import XCTest
import NIOCore
@testable import PostgresNIO

class JSON_PSQLCodableTests: XCTestCase {
    
    struct Hello: Equatable, Codable, PSQLCodable {
        let hello: String
        
        init(name: String) {
            self.hello = name
        }
    }
    
    func testRoundTrip() {
        var buffer = ByteBuffer()
        let hello = Hello(name: "world")
        XCTAssertNoThrow(try hello.encode(into: &buffer, context: .forTests()))
        XCTAssertEqual(hello.psqlType, .jsonb)
        
        // verify jsonb prefix byte
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 1)

        var result: Hello?
        XCTAssertNoThrow(result = try Hello.decode(from: &buffer, type: .jsonb, format: .binary, context: .forTests()))
        XCTAssertEqual(result, hello)
    }
    
    func testDecodeFromJSON() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        var result: Hello?
        XCTAssertNoThrow(result = try Hello.decode(from: &buffer, type: .json, format: .binary, context: .forTests()))
        XCTAssertEqual(result, Hello(name: "world"))
    }
    
    func testDecodeFromJSONAsText() {
        let combinations : [(PSQLFormat, PSQLDataType)] = [
            (.text, .json), (.text, .jsonb),
        ]
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)
        
        for (format, dataType) in combinations {
            var loopBuffer = buffer
            var result: Hello?
            XCTAssertNoThrow(result = try Hello.decode(from: &loopBuffer, type: dataType, format: format, context: .forTests()))
            XCTAssertEqual(result, Hello(name: "world"))
        }
    }
    
    func testDecodeFromJSONBWithoutVersionPrefixByte() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        XCTAssertThrowsError(try Hello.decode(from: &buffer, type: .jsonb, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .failure)
        }
    }
    
    func testDecodeFromJSONBWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)

        XCTAssertThrowsError(try Hello.decode(from: &buffer, type: .text, format: .binary, context: .forTests())) {
            XCTAssertEqual($0 as? PSQLCastingError.Code, .typeMismatch)
        }
    }
    
    func testCustomEncoderIsUsed() {
        class TestEncoder: PSQLJSONEncoder {
            var encodeHits = 0
            
            func encode<T>(_ value: T, into buffer: inout ByteBuffer) throws where T : Encodable {
                self.encodeHits += 1
            }
        }
        
        let hello = Hello(name: "world")
        let encoder = TestEncoder()
        var buffer = ByteBuffer()
        XCTAssertNoThrow(try hello.encode(into: &buffer, context: .forTests(jsonEncoder: encoder)))
        XCTAssertEqual(encoder.encodeHits, 1)
    }
}
