//
//  File.swift
//  
//
//  Created by Fabian Fett on 03.02.21.
//

import XCTest
@testable import PostgresNIO

class Bool_PSQLCodableTests: XCTestCase {
    
    func testTrueRoundTrip() {
        let value = true
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .bool)
        XCTAssertEqual(buffer.readableBytes, 1)
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 1)
        let data = PSQLData(bytes: buffer, dataType: .bool)
        
        var result: Bool?
        XCTAssertNoThrow(result = try data.decode(as: Bool.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testFalseRoundTrip() {
        let value = false
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .bool)
        XCTAssertEqual(buffer.readableBytes, 1)
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 0)
        let data = PSQLData(bytes: buffer, dataType: .bool)
        
        var result: Bool?
        XCTAssertNoThrow(result = try data.decode(as: Bool.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }

    func testDecodeBoolInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(1))
        let data = PSQLData(bytes: buffer, dataType: .bool)
        
        XCTAssertThrowsError(try data.decode(as: Bool.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))
        let data = PSQLData(bytes: buffer, dataType: .bool)
        
        XCTAssertThrowsError(try data.decode(as: Bool.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
}
