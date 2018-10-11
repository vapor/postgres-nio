public struct PostgresBinds {
    public var data: [PostgresData]
    
    public init() {
        self.data = []
    }
    
    public mutating func encode<T>(_ encodable: T)
        where T: PostgresDataEncodable
    {
        var data: PostgresData?
        encodable.encode(to: &data)
        self.data.append(data ?? .null)
    }
}
