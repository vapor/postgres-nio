extension PostgresData {
    public init(float: Float) {
        self.init(double: Double(float))
    }
    
    public var float: Float? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .float4:
                return value.readFloat()
            case .float8:
                return value.readDouble()
                    .flatMap { Float($0) }
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Float(string)
        }
    }
}

extension Float: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .float8
    }

    public var postgresData: PostgresData? {
        return .init(float: self)
    }

    public init?(postgresData: PostgresData) {
        guard let float = postgresData.float else {
            return nil
        }
        self = float
    }
}
