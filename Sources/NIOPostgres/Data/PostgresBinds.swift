public struct PostgresBinds {
    public var data: [PostgresData]
    
    public init() {
        self.data = []
    }
    
    public mutating func encode(_ encodable: PostgresDataConvertible) {
        self.data.append(encodable.postgresData)
    }
}

extension PostgresBinds: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: PostgresDataConvertible...) {
        self.init()
        for element in elements {
            self.encode(element)
        }
    }
}
