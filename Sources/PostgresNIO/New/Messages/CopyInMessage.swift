extension PostgresBackendMessage {
    struct CopyInResponse: Hashable {
        enum Format: Int8 {
            case textual = 0
            case binary = 1
        }

        let format: Format
        let columnFormats: [Format]

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let rawFormat = buffer.readInteger(endianness: .big, as: Int8.self) else {
                throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(1, actual: buffer.readableBytes)
            }
            guard let format = Format(rawValue: rawFormat) else {
                throw PSQLPartialDecodingError.unexpectedValue(value: rawFormat)
            }
            
            guard let numColumns = buffer.readInteger(endianness: .big, as: Int16.self) else {
                throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(2, actual: buffer.readableBytes)
            }
            var columnFormatCodes: [Format] = []
            columnFormatCodes.reserveCapacity(Int(numColumns))

            for _ in 0..<numColumns {
                guard let rawColumnFormat = buffer.readInteger(endianness: .big, as: Int16.self) else {
                    throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(2, actual: buffer.readableBytes)
                }
                guard Int8.min <= rawColumnFormat, rawColumnFormat <= Int8.max, let columnFormat = Format(rawValue: Int8(rawColumnFormat)) else {
                    throw PSQLPartialDecodingError.unexpectedValue(value: rawColumnFormat)
                }
                columnFormatCodes.append(columnFormat)
            }
            
            return CopyInResponse(format: format, columnFormats: columnFormatCodes)
        }
    }
}

extension PostgresBackendMessage.CopyInResponse: CustomDebugStringConvertible {
    var debugDescription: String {
        "format: \(format), columnFormats: \(columnFormats)"
    }
}
