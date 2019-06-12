extension PostgresData {
    public init(bool: Bool) {
        var buffer = ByteBufferAllocator().buffer(capacity: 1)
        buffer.writeInteger(bool ? 1 : 0, as: UInt8.self)
        self.init(type: .bool, formatCode: .binary, value: buffer)
    }
    
    public var bool: Bool? {
        guard var value = self.value else {
            return nil
        }
        guard value.readableBytes == 1 else {
            return nil
        }
        guard let byte = value.readInteger(as: UInt8.self) else {
            return nil
        }
        
        switch self.formatCode {
        case .text:
            switch byte {
            case Character("t").asciiValue!:
                return true
            case Character("f").asciiValue!:
                return false
            default:
                return nil
            }
        case .binary:
            switch byte {
            case 1:
                return true
            case 0:
                return false
            default:
                return nil
            }
        }
    }
}

extension PostgresData: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self.init(bool: value)
    }
}

extension Bool: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .bool
    }
    
    public var postgresData: PostgresData? {
        return .init(bool: self)
    }
    
    public init?(postgresData: PostgresData) {
        guard let bool = postgresData.bool else {
            return nil
        }
        self = bool
    }
}
