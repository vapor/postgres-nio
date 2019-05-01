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
            case .varchar, .text:
                guard let string = value.readString(length: value.readableBytes) else {
                    fatalError()
                }
                return string
            case .numeric:
                return self.numeric?.description
            case .uuid: return value.readUUID()!.uuidString
            default: fatalError("Cannot decode String from \(self)")
            }
        case .text:
            guard let string = value.readString(length: value.readableBytes) else {
                fatalError()
            }
            return string
        }
    }
}

extension PostgresData: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(string: value)
    }
}
