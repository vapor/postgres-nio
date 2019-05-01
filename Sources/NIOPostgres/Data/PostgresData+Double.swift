extension PostgresData {
    public init(double: Double) {
        fatalError()
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
            default: fatalError("Cannot decode Double from \(self)")
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Double(string)
        }
    }
}
