//
//  File.swift
//  
//
//  Created by Fabian Fett on 12.01.21.
//

import NIO
import XCTest
@testable import PostgresNIO

class PSQLDataTests: XCTestCase {
    func testStringDecoding() {
        let emptyBuffer: ByteBuffer? = nil
        
        let data = PSQLData(bytes: emptyBuffer, dataType: .text)
        
        var emptyResult: String?
        XCTAssertNoThrow(emptyResult = try data.decodeIfPresent(as: String.self, context: .forTests()))
        XCTAssertNil(emptyResult)
        
        XCTAssertNoThrow(emptyResult = try data.decode(as: String?.self, context: .forTests()))
        XCTAssertNil(emptyResult)
    }
}
