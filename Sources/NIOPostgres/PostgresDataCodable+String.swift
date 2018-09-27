extension String: PostgresDataCodable {
    public static func decode(from data: PostgresData) -> String? {
        guard let value = data.value else {
            fatalError()
        }
        switch data.formatCode {
        case .binary:
            switch data.type {
            case .varchar, .text: return String(bytes: value, encoding: .utf8)!
            default: fatalError()
            }
        case .text: return String(bytes: value, encoding: .utf8)!
        }
    }
    
    public func encode(to data: inout PostgresData?) {
        data = PostgresData(type: .text, typeModifier: 0, formatCode: .binary, value: .init(utf8))
    }
}
