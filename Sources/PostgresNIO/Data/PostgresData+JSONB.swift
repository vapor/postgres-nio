import Foundation

fileprivate let jsonBVersionBytes: [UInt8] = [0x01]

extension PostgresData {
    public init(jsonb jsonData: Data) {
        let jsonBDataBytes = [UInt8](jsonData)
        
        var buffer = ByteBufferAllocator().buffer(capacity: jsonBVersionBytes.count + jsonBDataBytes.count)
        buffer.writeBytes(jsonBVersionBytes)
        buffer.writeBytes(jsonBDataBytes)
        
        self.init(type: .jsonb, formatCode: .binary, value: buffer)
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
}

public protocol JSONBCodable: Codable, PostgresDataConvertible {
}

extension JSONBCodable {
    public static var postgresDataType: PostgresDataType {
        return .jsonb
    }
    
    public var postgresData: PostgresData? {
        guard let jsonData = try? JSONEncoder().encode(self) else {
            return nil
        }
        
        return .init(jsonb: jsonData)
    }
    
    public init?(postgresData: PostgresData) {
        guard let jsonData = postgresData.jsonb else {
            return nil
        }
        
        guard let value = try? JSONDecoder().decode(Self.self, from: jsonData) else {
            return nil
        }
        
        self = value
    }
}
