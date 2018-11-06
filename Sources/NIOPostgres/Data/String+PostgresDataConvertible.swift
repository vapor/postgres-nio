import NIO

extension String: PostgresDataConvertible {
    public init?(postgresData: PostgresData) {
        guard let value = postgresData.value else {
            return nil
        }
        switch postgresData.formatCode {
        case .binary:
            switch postgresData.type {
            case .varchar, .text:
                guard let string = value.getString(at: value.readerIndex, length: value.readableBytes) else {
                    return nil
                }
                self = string
            default:
                fatalError("Cannot decode String from \(postgresData)")
            }
        case .text:
            guard let string = value.getString(at: value.readerIndex, length: value.readableBytes) else {
                return nil
            }
            self = string
        }
    }
    
    public var postgresData: PostgresData? {
        #warning("should not be creating an allocator here")
        var buffer = ByteBufferAllocator.init().buffer(capacity: utf8.count)
        buffer.write(string: self)
        return PostgresData(type: .text, formatCode: .binary, value: buffer)
    }
}
