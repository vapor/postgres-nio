import NIOCore
import NIOEmbedded
import NIOTestUtils
import Testing

@testable import PostgresNIO

@Suite
struct PSQLBackendMessageTests {

    // MARK: ID

    @Test func initMessageIDWithBytes() {
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "R")) == .authentication)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "K")) == .backendKeyData)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "2")) == .bindComplete)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "3")) == .closeComplete)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "C")) == .commandComplete)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "d")) == .copyData)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "c")) == .copyDone)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "G")) == .copyInResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "H")) == .copyOutResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "W")) == .copyBothResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "D")) == .dataRow)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "I")) == .emptyQueryResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "E")) == .error)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "V")) == .functionCallResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "v")) == .negotiateProtocolVersion)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "n")) == .noData)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "N")) == .noticeResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "A")) == .notificationResponse)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "t")) == .parameterDescription)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "S")) == .parameterStatus)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "1")) == .parseComplete)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "s")) == .portalSuspended)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "Z")) == .readyForQuery)
        #expect(PostgresBackendMessage.ID(rawValue: UInt8(ascii: "T")) == .rowDescription)

        #expect(PostgresBackendMessage.ID(rawValue: 0) == nil)
    }

    @Test func messageIDHasCorrectRawValue() {
        #expect(PostgresBackendMessage.ID.authentication.rawValue == UInt8(ascii: "R"))
        #expect(PostgresBackendMessage.ID.backendKeyData.rawValue == UInt8(ascii: "K"))
        #expect(PostgresBackendMessage.ID.bindComplete.rawValue == UInt8(ascii: "2"))
        #expect(PostgresBackendMessage.ID.closeComplete.rawValue == UInt8(ascii: "3"))
        #expect(PostgresBackendMessage.ID.commandComplete.rawValue == UInt8(ascii: "C"))
        #expect(PostgresBackendMessage.ID.copyData.rawValue == UInt8(ascii: "d"))
        #expect(PostgresBackendMessage.ID.copyDone.rawValue == UInt8(ascii: "c"))
        #expect(PostgresBackendMessage.ID.copyInResponse.rawValue == UInt8(ascii: "G"))
        #expect(PostgresBackendMessage.ID.copyOutResponse.rawValue == UInt8(ascii: "H"))
        #expect(PostgresBackendMessage.ID.copyBothResponse.rawValue == UInt8(ascii: "W"))
        #expect(PostgresBackendMessage.ID.dataRow.rawValue == UInt8(ascii: "D"))
        #expect(PostgresBackendMessage.ID.emptyQueryResponse.rawValue == UInt8(ascii: "I"))
        #expect(PostgresBackendMessage.ID.error.rawValue == UInt8(ascii: "E"))
        #expect(PostgresBackendMessage.ID.functionCallResponse.rawValue == UInt8(ascii: "V"))
        #expect(PostgresBackendMessage.ID.negotiateProtocolVersion.rawValue == UInt8(ascii: "v"))
        #expect(PostgresBackendMessage.ID.noData.rawValue == UInt8(ascii: "n"))
        #expect(PostgresBackendMessage.ID.noticeResponse.rawValue == UInt8(ascii: "N"))
        #expect(PostgresBackendMessage.ID.notificationResponse.rawValue == UInt8(ascii: "A"))
        #expect(PostgresBackendMessage.ID.parameterDescription.rawValue == UInt8(ascii: "t"))
        #expect(PostgresBackendMessage.ID.parameterStatus.rawValue == UInt8(ascii: "S"))
        #expect(PostgresBackendMessage.ID.parseComplete.rawValue == UInt8(ascii: "1"))
        #expect(PostgresBackendMessage.ID.portalSuspended.rawValue == UInt8(ascii: "s"))
        #expect(PostgresBackendMessage.ID.readyForQuery.rawValue == UInt8(ascii: "Z"))
        #expect(PostgresBackendMessage.ID.rowDescription.rawValue == UInt8(ascii: "T"))
    }

    // MARK: Decoder

    @Test func sslSupportedAsFirstByte() throws {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "S"))

        var expectedMessages: [PostgresBackendMessage] = [.sslSupported]

        // we test tons of ParameterStatus messages after the SSLSupported message, since those are
        // also identified by an "S"
        let parameterStatuses: [PostgresBackendMessage.ParameterStatus] = [
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

        for parameterStatus in parameterStatuses {
            buffer.writeBackendMessage(id: .parameterStatus) { buffer in
                buffer.writeNullTerminatedString(parameterStatus.parameter)
                buffer.writeNullTerminatedString(parameterStatus.value)
            }

            expectedMessages.append(.parameterStatus(parameterStatus))
        }

        let handler = ByteToMessageHandler(PostgresBackendMessageDecoder())
        let embedded = EmbeddedChannel(handler: handler)
        try embedded.writeInbound(buffer)

        for expected in expectedMessages {
            let message = try embedded.readInbound(as: PostgresBackendMessage.self)
            #expect(message == expected)
        }
    }

    @Test func sslUnsupportedAsFirstByte() throws {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "N"))

        // we test a NoticeResponse messages after the SSLUnupported message, since NoticeResponse
        // is identified by a "N"
        let fields: [PostgresBackendMessage.Field: String] = [
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
            for (key, value) in fields {
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self)  // signal done
        }

        let handler = ByteToMessageHandler(PostgresBackendMessageDecoder())
        let embedded = EmbeddedChannel(handler: handler)
        try embedded.writeInbound(buffer)

        for expected in expectedMessages {
            let message = try embedded.readInbound(as: PostgresBackendMessage.self)
            #expect(message == expected)
        }
    }

    @Test func payloadsWithoutAssociatedValues() {
        let messageIDs: [PostgresBackendMessage.ID] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]

        var buffer = ByteBuffer()
        for messageID in messageIDs {
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

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }
    }

    @Test func payloadsWithoutAssociatedValuesInvalidLength() {
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

            #expect(throws: PostgresMessageDecodingError.self) {
                try ByteToMessageDecoderVerifier.verifyDecoder(
                    inputOutputPairs: [(buffer, [])],
                    decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
                )
            }
        }
    }

    @Test func decodeCommandCompleteMessage() throws {
        let expected: [PostgresBackendMessage] = [
            .commandComplete("SELECT 100"),
            .commandComplete("INSERT 0 1"),
            .commandComplete("UPDATE 1"),
            .commandComplete("DELETE 1")
        ]

        var okBuffer = ByteBuffer()
        for message in expected {
            guard case .commandComplete(let commandTag) = message else {
                Issue.record("Programming error!")
                return
            }

            okBuffer.writeBackendMessage(id: .commandComplete) { buffer in
                buffer.writeNullTerminatedString(commandTag)
            }
        }

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(okBuffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }

        // test commandTag is not null terminated
        for message in expected {
            guard case .commandComplete(let commandTag) = message else {
                Issue.record("Programming error!")
                return
            }

            var failBuffer = ByteBuffer()
            failBuffer.writeBackendMessage(id: .commandComplete) { buffer in
                buffer.writeString(commandTag)
            }

            #expect(throws: PostgresMessageDecodingError.self) {
                try ByteToMessageDecoderVerifier.verifyDecoder(
                    inputOutputPairs: [(failBuffer, [])],
                    decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
                )
            }
        }
    }

    @Test func decodeMessageWithUnknownMessageID() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "x"))
        buffer.writeInteger(Int32(4))

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: false) }
            )
        }
    }

    @Test func debugDescription() {
        let salt: UInt32 = 0x00_01_02_03
        #expect("\(PostgresBackendMessage.authentication(.ok))" == ".authentication(.ok)")
        #expect("\(PostgresBackendMessage.authentication(.kerberosV5))" == ".authentication(.kerberosV5)")
        #expect("\(PostgresBackendMessage.authentication(.md5(salt: salt)))" == ".authentication(.md5(salt: \(salt)))")
        #expect("\(PostgresBackendMessage.authentication(.plaintext))" == ".authentication(.plaintext)")
        #expect("\(PostgresBackendMessage.authentication(.scmCredential))" == ".authentication(.scmCredential)")
        #expect("\(PostgresBackendMessage.authentication(.gss))" == ".authentication(.gss)")
        #expect("\(PostgresBackendMessage.authentication(.sspi))" == ".authentication(.sspi)")

        #expect(
            "\(PostgresBackendMessage.parameterStatus(.init(parameter: "foo", value: "bar")))"
                == #".parameterStatus(parameter: "foo", value: "bar")"#
        )
        #expect(
            "\(PostgresBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567)))"
                == ".backendKeyData(processID: 1234, secretKey: 4567)"
        )

        #expect("\(PostgresBackendMessage.bindComplete)" == ".bindComplete")
        #expect("\(PostgresBackendMessage.closeComplete)" == ".closeComplete")
        #expect("\(PostgresBackendMessage.commandComplete("SELECT 123"))" == #".commandComplete("SELECT 123")"#)
        #expect("\(PostgresBackendMessage.emptyQueryResponse)" == ".emptyQueryResponse")
        #expect("\(PostgresBackendMessage.noData)" == ".noData")
        #expect("\(PostgresBackendMessage.parseComplete)" == ".parseComplete")
        #expect("\(PostgresBackendMessage.portalSuspended)" == ".portalSuspended")

        #expect("\(PostgresBackendMessage.readyForQuery(.idle))" == ".readyForQuery(.idle)")
        #expect("\(PostgresBackendMessage.readyForQuery(.inTransaction))" == ".readyForQuery(.inTransaction)")
        #expect("\(PostgresBackendMessage.readyForQuery(.inFailedTransaction))" == ".readyForQuery(.inFailedTransaction)")
        #expect("\(PostgresBackendMessage.sslSupported)" == ".sslSupported")
        #expect("\(PostgresBackendMessage.sslUnsupported)" == ".sslUnsupported")
    }
}
