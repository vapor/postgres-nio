import NIOCore
import NIOTestUtils
import Testing

@testable import PostgresNIO

@Suite
struct RowDescriptionTests {
    @Test func decode() {
        let columns: [RowDescription.Column] = [
            .init(name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary),
            .init(name: "Second", tableOID: 123, columnAttributeNumber: 456, dataType: .uuidArray, dataTypeSize: 567, dataTypeModifier: 123, format: .text),
        ]

        let expected: [PostgresBackendMessage] = [
            .rowDescription(.init(columns: columns))
        ]

        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(columns.count))

            for column in columns {
                buffer.writeNullTerminatedString(column.name)
                buffer.writeInteger(column.tableOID)
                buffer.writeInteger(column.columnAttributeNumber)
                buffer.writeInteger(column.dataType.rawValue)
                buffer.writeInteger(column.dataTypeSize)
                buffer.writeInteger(column.dataTypeModifier)
                buffer.writeInteger(column.format.rawValue)
            }
        }

        #expect(throws: Never.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, expected)],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeFailureBecauseOfMissingNullTerminationInColumnName() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary
        )

        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(1))
            buffer.writeString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeFailureBecauseOfMissingColumnCount() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary
        )

        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeFailureBecauseInvalidFormatCode() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary
        )

        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(1))
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(UInt16(2))
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }

    @Test func decodeFailureBecauseNegativeColumnCount() {
        let column = RowDescription.Column(
            name: "First", tableOID: 123, columnAttributeNumber: 123, dataType: .bool, dataTypeSize: 2, dataTypeModifier: 8, format: .binary
        )

        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .rowDescription) { buffer in
            buffer.writeInteger(Int16(-1))
            buffer.writeNullTerminatedString(column.name)
            buffer.writeInteger(column.tableOID)
            buffer.writeInteger(column.columnAttributeNumber)
            buffer.writeInteger(column.dataType.rawValue)
            buffer.writeInteger(column.dataTypeSize)
            buffer.writeInteger(column.dataTypeModifier)
            buffer.writeInteger(column.format.rawValue)
        }

        #expect(throws: PostgresMessageDecodingError.self) {
            try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PostgresBackendMessageDecoder(hasAlreadyReceivedBytes: true) }
            )
        }
    }
}
