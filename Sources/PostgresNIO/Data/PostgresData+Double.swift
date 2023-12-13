import NIOCore

extension PostgresData {
    public init(double: Double) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.psqlWriteDouble(double)
        self.init(type: .float8, formatCode: .binary, value: buffer)
    }
    
    public var double: Double? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .float4:
                return value.psqlReadFloat()
                    .flatMap { Double($0) }
            case .float8:
                return value.psqlReadDouble()
            case .numeric:
                return self.numeric?.double
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Double(string)
        }
    }
}
