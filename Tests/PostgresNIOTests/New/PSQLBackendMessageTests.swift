import NIOCore
import NIOEmbedded
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class PSQLBackendMessageTests: XCTestCase {
    
    // MARK: ID
    
    func testInitMessageIDWithBytes() {
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "R")), .authentication)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "K")), .backendKeyData)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "2")), .bindComplete)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "3")), .closeComplete)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "C")), .commandComplete)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "d")), .copyData)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "c")), .copyDone)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "G")), .copyInResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "H")), .copyOutResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "W")), .copyBothResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "D")), .dataRow)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "I")), .emptyQueryResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "E")), .error)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "V")), .functionCallResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "v")), .negotiateProtocolVersion)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "n")), .noData)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "N")), .noticeResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "A")), .notificationResponse)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "t")), .parameterDescription)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "S")), .parameterStatus)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "1")), .parseComplete)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "s")), .portalSuspended)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "Z")), .readyForQuery)
        XCTAssertEqual(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "T")), .rowDescription)
        
        XCTAssertNil(PostgresBackendMessage.ID(rawValue: 0))
    }
    
    func testMessageIDHasCorrectRawValue() {
        XCTAssertEqual(PostgresBackendMessage.ID.authentication.rawValue, UInt8(ascii: "R"))
        XCTAssertEqual(PostgresBackendMessage.ID.backendKeyData.rawValue, UInt8(ascii: "K"))
        XCTAssertEqual(PostgresBackendMessage.ID.bindComplete.rawValue, UInt8(ascii: "2"))
        XCTAssertEqual(PostgresBackendMessage.ID.closeComplete.rawValue, UInt8(ascii: "3"))
        XCTAssertEqual(PostgresBackendMessage.ID.commandComplete.rawValue, UInt8(ascii: "C"))
        XCTAssertEqual(PostgresBackendMessage.ID.copyData.rawValue, UInt8(ascii: "d"))
        XCTAssertEqual(PostgresBackendMessage.ID.copyDone.rawValue, UInt8(ascii: "c"))
        XCTAssertEqual(PostgresBackendMessage.ID.copyInResponse.rawValue, UInt8(ascii: "G"))
        XCTAssertEqual(PostgresBackendMessage.ID.copyOutResponse.rawValue, UInt8(ascii: "H"))
        XCTAssertEqual(PostgresBackendMessage.ID.copyBothResponse.rawValue, UInt8(ascii: "W"))
        XCTAssertEqual(PostgresBackendMessage.ID.dataRow.rawValue, UInt8(ascii: "D"))
        XCTAssertEqual(PostgresBackendMessage.ID.emptyQueryResponse.rawValue, UInt8(ascii: "I"))
        XCTAssertEqual(PostgresBackendMessage.ID.error.rawValue, UInt8(ascii: "E"))
        XCTAssertEqual(PostgresBackendMessage.ID.functionCallResponse.rawValue, UInt8(ascii: "V"))
        XCTAssertEqual(PostgresBackendMessage.ID.negotiateProtocolVersion.rawValue, UInt8(ascii: "v"))
        XCTAssertEqual(PostgresBackendMessage.ID.noData.rawValue, UInt8(ascii: "n"))
        XCTAssertEqual(PostgresBackendMessage.ID.noticeResponse.rawValue, UInt8(ascii: "N"))
        XCTAssertEqual(PostgresBackendMessage.ID.notificationResponse.rawValue, UInt8(ascii: "A"))
        XCTAssertEqual(PostgresBackendMessage.ID.parameterDescription.rawValue, UInt8(ascii: "t"))
        XCTAssertEqual(PostgresBackendMessage.ID.parameterStatus.rawValue, UInt8(ascii: "S"))
        XCTAssertEqual(PostgresBackendMessage.ID.parseComplete.rawValue, UInt8(ascii: "1"))
        XCTAssertEqual(PostgresBackendMessage.ID.portalSuspended.rawValue, UInt8(ascii: "s"))
        XCTAssertEqual(PostgresBackendMessage.ID.readyForQuery.rawValue, UInt8(ascii: "Z"))
        XCTAssertEqual(PostgresBackendMessage.ID.rowDescription.rawValue, UInt8(ascii: "T"))
    }
    
    // MARK: Decoder
    
    func testSSLSupportedAsFirstByte() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "S"))
        
        var expectedMessages: [PostgresBackendMessage] = [.sslSupported]
        
        // we test tons of ParameterStatus messages after the SSLSupported message, since those are
        // also identified by an "S"
        let parameterStatus: [PostgresBackendMessage.ParameterStatus] = [
            .init(parameter: "DateStyle", value: "ISO, MDY"),
            .init(parameter: "application_name", value: ""),
            .init(parameter: "server_encoding", value: "UTF8"),
            .init(parameter: "integer_datetimes", value: "on"),
            .init(parameter: "client_encoding", value: "UTF8"),
            .init(parameter: "TimeZone", value: "Etc/UTC"),
            .init(parameter: "is_superuser", value: "on"),
            .init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)"),
            .init(parameter: "session_authorization", value: "postgres"),
            .init(parameter: "IntervalStyle", value: "postgres"),
            .init(parameter: "standard_conforming_strings", value: "on"),
        ]
        
        parameterStatus.forEach { parameterStatus in
            buffer.writeBackendMessage(id: .parameterStatus) { buffer in
                buffer.writeNullTerminatedString(parameterStatus.parameter)
                buffer.writeNullTerminatedString(parameterStatus.value)
            }
            
            expectedMessages.append(.parameterStatus(parameterStatus))
        }
        
        let handler = ByteToMessageHandler(PostgresBackendMessageDecoder())
        let embedded = EmbeddedChannel(handler: handler)
        XCTAssertNoThrow(try embedded.writeInbound(buffer))
        
        for expected in expectedMessages {
            var message: PostgresBackendMessage?
            XCTAssertNoThrow(message = try embedded.readInbound(as: PostgresBackendMessage.self))
            XCTAssertEqual(message, expected)
        }
    }

    func testSSLUnsupportedAsFirstByte() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "N"))
        
        // we test a NoticeResponse messages after the SSLUnupported message, since NoticeResponse
        // is identified by a "N"
        let fields: [PostgresBackendMessage.Field : String] = [
            .file: "auth.c",
            .routine: "auth_failed",
            .line: "334",
            .localizedSeverity: "FATAL",
            .sqlState: "28P01",
            .severity: "FATAL",
            .message: "password authentication failed for user \"postgre3\"",
        ]
        
        let expectedMessages: [PostgresBackendMessage] = [
            .sslUnsupported,
            .notice(.init(fields: fields))
        ]
        
        buffer.writeBackendMessage(id: .noticeResponse) { buffer in
            fields.forEach { (key, value) in
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self) // signal done
        }
        
        let handler = ByteToMessageHandler(PostgresBackendMessageDecoder())
        let embedded = EmbeddedChannel(handler: handler)
        XCTAssertNoThrow(try embedded.writeInbound(buffer))
        
        for expected in expectedMessages {
            var message: PostgresBackendMessage?
            XCTAssertNoThrow(message = try embedded.readInbound(as: PostgresBackendMessage.self))
            XCTAssertEqual(message, expected)
        }
    }

    func testPayloadsWithoutAssociatedValues() {
        let messageIDs: [PostgresBackendMessage.ID] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]
        
        var buffer = ByteBuffer()
        messageIDs.forEach { messageID in
            buffer.writeBackendMessage(id: messageID) { _ in }
        }
        
        let expected: [PostgresBackendMessage] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }))
    }
    
    func testPayloadsWithoutAssociatedValuesInvalidLength() {
        let messageIDs: [PostgresBackendMessage.ID] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]
        
        for messageID in messageIDs {
            var buffer = ByteBuffer()
            buffer.writeBackendMessage(id: messageID) { buffer in
                buffer.writeInteger(UInt8(0))
            }
            
            XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) })) {
                XCTAssert($0 is PSQLDecodingError)
            }
        }
    }
    
    func testDecodeCommandCompleteMessage() {
        let expected: [PostgresBackendMessage] = [
            .commandComplete("SELECT 100"),
            .commandComplete("INSERT 0 1"),
            .commandComplete("UPDATE 1"),
            .commandComplete("DELETE 1")
        ]
        
        var okBuffer = ByteBuffer()
        expected.forEach { message in
            guard case .commandComplete(let commandTag) = message else {
                return XCTFail("Programming error!")
            }
            
            okBuffer.writeBackendMessage(id: .commandComplete) { buffer in
                buffer.writeNullTerminatedString(commandTag)
            }
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(okBuffer, expected)],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }))
        
        // test commandTag is not null terminated
        for message in expected {
            guard case .commandComplete(let commandTag) = message else {
                return XCTFail("Programming error!")
            }
            
            var failBuffer = ByteBuffer()
            failBuffer.writeBackendMessage(id: .commandComplete) { buffer in
                buffer.writeString(commandTag)
            }
            
            XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(failBuffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) })) {
                XCTAssert($0 is PSQLDecodingError)
            }
        }
    }
    
    func testDecodeMessageWithUnknownMessageID() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "x"))
        buffer.writeInteger(Int32(4))
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) })) {
            XCTAssert($0 is PSQLDecodingError)
        }
    }
    
    func testDebugDescription() {
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.ok))", ".authentication(.ok)")
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.kerberosV5))",
                       ".authentication(.kerberosV5)")
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.md5(salt: (0, 1, 2, 3))))",
                       ".authentication(.md5(salt: (0, 1, 2, 3)))")
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.plaintext))",
                       ".authentication(.plaintext)")
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.scmCredential))",
                       ".authentication(.scmCredential)")
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.gss))",
                       ".authentication(.gss)")
        XCTAssertEqual("\(PostgresBackendMessage.authentication(.sspi))",
                       ".authentication(.sspi)")
        
        XCTAssertEqual("\(PostgresBackendMessage.parameterStatus(.init(parameter: "foo", value: "bar")))",
                       #".parameterStatus(parameter: "foo", value: "bar")"#)
        XCTAssertEqual("\(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567)))",
                       ".backendKeyData(processID: 1234, secretKey: 4567)")
        
        XCTAssertEqual("\(PostgresBackendMessage.bindComplete)", ".bindComplete")
        XCTAssertEqual("\(PostgresBackendMessage.closeComplete)", ".closeComplete")
        XCTAssertEqual("\(PostgresBackendMessage.commandComplete("SELECT 123"))", #".commandComplete("SELECT 123")"#)
        XCTAssertEqual("\(PostgresBackendMessage.emptyQueryResponse)", ".emptyQueryResponse")
        XCTAssertEqual("\(PostgresBackendMessage.noData)", ".noData")
        XCTAssertEqual("\(PostgresBackendMessage.parseComplete)", ".parseComplete")
        XCTAssertEqual("\(PostgresBackendMessage.portalSuspended)", ".portalSuspended")
        
        XCTAssertEqual("\(PostgresBackendMessage.readyForQuery(.idle))", ".readyForQuery(.idle)")
        XCTAssertEqual("\(PostgresBackendMessage.readyForQuery(.inTransaction))",
                       ".readyForQuery(.inTransaction)")
        XCTAssertEqual("\(PostgresBackendMessage.readyForQuery(.inFailedTransaction))",
                       ".readyForQuery(.inFailedTransaction)")
        XCTAssertEqual("\(PostgresBackendMessage.sslSupported)", ".sslSupported")
        XCTAssertEqual("\(PostgresBackendMessage.sslUnsupported)", ".sslUnsupported")
    }
    
}
