extension String: PostgresDataConvertible {
    public init(postgresData: PostgresData) {
        guard let value = postgresData.value else {
            fatalError()
        }
        switch postgresData.formatCode {
        case .binary:
            switch postgresData.type {
            case .varchar, .text:
                self = String(bytes: value, encoding: .utf8)!
            default: fatalError()
            }
        case .text:
            self = String(bytes: value, encoding: .utf8)!
        }
    }
    
    public var postgresData: PostgresData {
        return PostgresData(type: .text, typeModifier: 0, formatCode: .binary, value: .init(utf8))
    }
}
