import NIO

final class PostgresDataEncoder: Encoder {
    var codingPath: [CodingKey] {
        return []
    }
    
    var userInfo: [CodingUserInfoKey: Any] {
        return [:]
    }
    
    var data: PostgresData?
    let allocator: ByteBufferAllocator
    
    init(allocator: ByteBufferAllocator) {
        self.data = nil
        self.allocator = allocator
    }
    
    func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key : CodingKey {
        fatalError()
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        fatalError()
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        return SingleValueEncoder(encoder: self)
    }
    
    struct SingleValueEncoder: SingleValueEncodingContainer {
        var codingPath: [CodingKey] {
            return []
        }
        
        let encoder: PostgresDataEncoder
        
        mutating func encodeNil() throws {
            fatalError()
        }
        
        mutating func encode(_ value: Bool) throws {
            fatalError()
        }
        
        mutating func encode(_ value: String) throws {
            var buffer = self.encoder.allocator.buffer(capacity: value.utf8.count)
            buffer.write(string: value)
            self.encoder.data = PostgresData(type: .text, formatCode: .binary, value: buffer)
        }
        
        mutating func encode(_ value: Double) throws {
            fatalError()
        }
        
        mutating func encode(_ value: Float) throws {
            fatalError()
        }
        
        mutating func encode(_ value: Int) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: Int8) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: Int16) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: Int32) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: Int64) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: UInt) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: UInt8) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: UInt16) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: UInt32) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode(_ value: UInt64) throws {
            try self.encode(integer: value)
        }
        
        mutating func encode<I>(integer: I) throws where I: FixedWidthInteger {
            let capacity: Int
            let type: PostgresDataType
            switch I.bitWidth {
            case 8:
                capacity = 1
                type = .char
            case 16:
                capacity = 2
                type = .int2
            case 32:
                capacity = 3
                type = .int4
            case 64:
                capacity = 4
                type = .int8
            default:
                fatalError("Cannot encode \(I.self) to PostgresData")
            }
            var buffer = self.encoder.allocator.buffer(capacity: capacity)
            buffer.write(integer: integer)
            self.encoder.data = PostgresData(type: type, formatCode: .binary, value: buffer)
        }
        
        mutating func encode<T>(_ value: T) throws where T : Encodable {
            if let value = value as? PostgresDataConvertible {
                self.encoder.data = value.postgresData
            } else {
                try value.encode(to: self.encoder)
            }
        }
    }
}
