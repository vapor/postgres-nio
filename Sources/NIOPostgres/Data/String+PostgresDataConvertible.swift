import NIO

extension String: PostgresDataConvertible {
    public init?(postgresData: PostgresData) {
        guard var value = postgresData.value else {
            return nil
        }
        switch postgresData.formatCode {
        case .binary:
            switch postgresData.type {
            case .varchar, .text:
                guard let string = value.readString(length: value.readableBytes) else {
                    return nil
                }
                self = string
            default: fatalError("Cannot decode String from \(postgresData)")
            }
        case .text:
            guard let string = value.readString(length: value.readableBytes) else {
                return nil
            }
            self = string
        }
    }
    
    public var postgresData: PostgresData? {
        #warning("should we use channel allocator here?")
        var buffer = ByteBufferAllocator.init().buffer(capacity: utf8.count)
        buffer.write(string: self)
        return PostgresData(type: .text, formatCode: .binary, value: buffer)
    }
}
