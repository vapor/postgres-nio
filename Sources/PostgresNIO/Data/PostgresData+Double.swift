extension PostgresData {
    public init(double: Double) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeString(double.description)
        self.init(type: .float8, formatCode: .text, value: buffer)
    }
    
    public var double: Double? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .float4:
                return value.readFloat(as: Float.self)
                    .flatMap { Double($0) }
            case .float8:
                return value.readFloat(as: Double.self)
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
