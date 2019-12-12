import Foundation

fileprivate let jsonBVersionBytes: [UInt8] = [0x01]

extension PostgresData {
    public init(jsonb jsonData: Data) {
        let jsonBDataBytes = [UInt8](jsonData)
        
        var buffer = ByteBufferAllocator()
            .buffer(capacity: jsonBVersionBytes.count + jsonBDataBytes.count)
        buffer.writeBytes(jsonBVersionBytes)
        buffer.writeBytes(jsonBDataBytes)
        
        self.init(type: .jsonb, formatCode: .binary, value: buffer)
    }

    public init<T>(jsonb value: T) throws where T: Encodable {
        let jsonData = try JSONEncoder().encode(value)
        self.init(jsonb: jsonData)
    }

    public var jsonb: Data? {
        guard var value = self.value else {
            return nil
        }

        guard let versionBytes = value.readBytes(length: jsonBVersionBytes.count), [UInt8](versionBytes) == jsonBVersionBytes else {
            return nil
        }

        guard let dataBytes = value.readBytes(length: value.readableBytes) else {
            return nil
        }

        return Data(dataBytes)
    }

    public func jsonb<T>(as type: T.Type) throws -> T? where T: Decodable {
        guard let jsonData = jsonb else {
            return nil
        }

        return try JSONDecoder().decode(T.self, from: jsonData)
    }
}

public protocol PostgresJSONBCodable: Codable, PostgresDataConvertible {
}

extension PostgresJSONBCodable {
    public static var postgresDataType: PostgresDataType {
        return .jsonb
    }
    
    public var postgresData: PostgresData? {
        return try? .init(jsonb: self)
    }
    
    public init?(postgresData: PostgresData) {
        guard let value = try? postgresData.jsonb(as: Self.self) else {
            return nil
        }
        self = value
    }
}
