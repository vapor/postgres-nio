import NIOCore
import struct Foundation.Data

fileprivate let jsonBVersionBytes: [UInt8] = [0x01]

extension PostgresData {
    public init(jsonb jsonData: Data) {
        let jsonBData = [UInt8](jsonData)
        
        var buffer = ByteBufferAllocator()
            .buffer(capacity: jsonBVersionBytes.count + jsonBData.count)
        buffer.writeBytes(jsonBVersionBytes)
        buffer.writeBytes(jsonBData)
        
        self.init(type: .jsonb, formatCode: .binary, value: buffer)
    }

    public init<T>(jsonb value: T) throws where T: Encodable {
        let jsonData = try PostgresNIO._defaultJSONEncoder.encode(value)
        self.init(jsonb: jsonData)
    }

    public var jsonb: Data? {
        guard var value = self.value else {
            return nil
        }
        guard case .jsonb = self.type else {
            return nil
        }

        guard let versionBytes = value.readBytes(length: jsonBVersionBytes.count), [UInt8](versionBytes) == jsonBVersionBytes else {
            return nil
        }

        guard let data = value.readBytes(length: value.readableBytes) else {
            return nil
        }

        return Data(data)
    }

    public func jsonb<T>(as type: T.Type) throws -> T? where T: Decodable {
        guard let data = jsonb else {
            return nil
        }

        return try PostgresNIO._defaultJSONDecoder.decode(T.self, from: data)
    }
}

public protocol PostgresJSONBCodable: Codable, PostgresDataConvertible { }

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
