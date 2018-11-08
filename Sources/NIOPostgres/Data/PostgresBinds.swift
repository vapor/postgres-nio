import NIO

public struct PostgresBinds {
    var data: [Encodable]
    
    public init() {
        self.data = []
    }
    
    internal func serialize(allocator: ByteBufferAllocator) throws -> [PostgresData] {
        let encoder = PostgresDataEncoder(allocator: allocator)
        return try data.map { encodable in
            try encodable.encode(to: encoder)
            return encoder.data!
        }
    }
    
    public mutating func encode(_ encodable: Encodable) {
        self.data.append(encodable)
    }
}

extension PostgresBinds: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: Encodable...) {
        self.init()
        for element in elements {
            self.encode(element)
        }
    }
}
