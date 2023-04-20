import NIOCore

extension PostgresData {
    public init(string: String) {
        var buffer = ByteBufferAllocator().buffer(capacity: string.utf8.count)
        buffer.writeString(string)
        self.init(type: .text, formatCode: .binary, value: buffer)
    }
    
    public var string: String? {
        guard var value = self.value else {
            return nil
        }
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .varchar, .text, .name:
                guard let string = value.readString(length: value.readableBytes) else {
                    return nil
                }
                return string
            case .numeric:
                return self.numeric?.string
            case .uuid:
                return value.readUUIDBytes()!.uuidString
            case .timestamp, .timestamptz, .date:
                return self.date?.description
            case .money:
                assert(value.readableBytes == 8)
                guard let int64 = value.getInteger(at: value.readerIndex, as: Int64.self) else {
                    return nil
                }
                let description = int64.description
                switch description.count {
                case 0:
                    return "0.00"
                case 1:
                    return "0.0" + description
                case 2:
                    return "0." + description
                default:
                    let decimalIndex = description.index(description.endIndex, offsetBy: -2)
                    return description[description.startIndex..<decimalIndex]
                        + "."
                        + description[decimalIndex..<description.endIndex]
                }
            case .float4, .float8:
                return self.double?.description
            case .int2, .int4, .int8:
                return self.int?.description
            case .bpchar:
                return value.readString(length: value.readableBytes)
            default:
                if self.type.isUserDefined {
                    // custom type
                    return value.readString(length: value.readableBytes)
                } else {
                    return nil
                }
            }
        case .text:
            guard let string = value.readString(length: value.readableBytes) else {
                return nil
            }
            return string
        }
    }
    
    public var character: Character? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .bpchar:
                guard let byte = value.readInteger(as: UInt8.self) else {
                    return nil
                }
                return Character(UnicodeScalar(byte))
            default:
                return nil
            }
        case .text:
            return nil
        }
    }
}

extension PostgresData: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(string: value)
    }
}

@available(*, deprecated, message: "Deprecating conformance to `PostgresDataConvertible`, since it is deprecated.")
extension String: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .text
    }
    
    public var postgresData: PostgresData? {
        return .init(string: self)
    }

    public init?(postgresData: PostgresData) {
        guard let string = postgresData.string else {
            return nil
        }
        self = string
    }
}
