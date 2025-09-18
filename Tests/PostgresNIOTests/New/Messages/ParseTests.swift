import Testing
import NIOCore
@testable import PostgresNIO

@Suite struct ParseTests {
    @Test func testEncode() {
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
        
        #expect(byteBuffer.readableBytes == length)
        #expect(byteBuffer.readInteger(as: UInt8.self) == PostgresFrontendMessage.ID.parse.rawValue)
        #expect(byteBuffer.readInteger(as: Int32.self) == Int32(length - 1))
        #expect(byteBuffer.readNullTerminatedString() == preparedStatementName)
        #expect(byteBuffer.readNullTerminatedString() == query)
        #expect(byteBuffer.readInteger(as: UInt16.self) == UInt16(parameters.count))
        for dataType in parameters {
            #expect(byteBuffer.readInteger(as: UInt32.self) == dataType.rawValue)
        }
    }
}
