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
                return value.readFloat()
                    .flatMap { Double($0) }
            case .float8:
                return value.readDouble()
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

extension Double: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .float8
    }

    public var postgresData: PostgresData? {
        return .init(double: self)
    }

    public init?(postgresData: PostgresData) {
        guard let double = postgresData.double else {
            return nil
        }
        self = double
    }
}
