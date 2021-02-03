//
//  File.swift
//  
//
//  Created by Fabian Fett on 03.02.21.
//

import XCTest
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
        
        let data = PSQLData(bytes: buffer, dataType: .jsonb)
        var result: Hello?
        XCTAssertNoThrow(result = try data.decode(as: Hello.self, context: .forTests()))
        XCTAssertEqual(result, hello)
    }
    
    func testDecodeFromJSON() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)
        
        let data = PSQLData(bytes: buffer, dataType: .json)
        var result: Hello?
        XCTAssertNoThrow(result = try data.decode(as: Hello.self, context: .forTests()))
        XCTAssertEqual(result, Hello(name: "world"))
    }
    
    func testDecodeFromJSONBWithoutVersionPrefixByte() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)
        
        let data = PSQLData(bytes: buffer, dataType: .jsonb)
        XCTAssertThrowsError(try data.decode(as: Hello.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeFromJSONBWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeString(#"{"hello":"world"}"#)
        
        let data = PSQLData(bytes: buffer, dataType: .text)
        XCTAssertThrowsError(try data.decode(as: Hello.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
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
