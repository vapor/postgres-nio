import XCTest
import NIOCore
@testable import PostgresNIO

class ParseTests: XCTestCase {
    func testEncode() {
        let preparedStatementName = "test"
        let query = "SELECT version()"
        let parameters: [PostgresDataType] = [.bool, .int8, .bytea, .varchar, .text, .uuid, .json, .jsonbArray]
        var encoder = PostgresFrontendMessageEncoder(buffer: .init())
        encoder.parse(
            preparedStatementName: preparedStatementName,
            query: query,
            parameters: parameters
        )
        var byteBuffer = encoder.flushBuffer()
        
        let length: Int = 1 + 4 + (preparedStatementName.count + 1) + (query.count + 1) + 2 + parameters.count * 4

        //   1 id
        // + 4 length
        // + 4 preparedStatement (3 + 1 null terminator)
        // + 1 query ()
        
        XCTAssertEqual(byteBuffer.readableBytes, length)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PostgresFrontendMessage.ID.parse.rawValue)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(length - 1))
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), preparedStatementName)
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), query)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt16.self), UInt16(parameters.count))
        for dataType in parameters {
            XCTAssertEqual(byteBuffer.readInteger(as: UInt32.self), dataType.rawValue)
        }
    }
}
