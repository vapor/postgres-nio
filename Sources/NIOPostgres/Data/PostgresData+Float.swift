extension PostgresData {
    public init(float: Float) {
        fatalError()
    }
    
    public var float: Float? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .float4:
                return value.readFloat(as: Float.self)
            case .float8:
                return value.readFloat(as: Double.self)
                    .flatMap { Float($0) }
            default: fatalError("Cannot decode Float from \(self)")
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Float(string)
        }
    }
}
