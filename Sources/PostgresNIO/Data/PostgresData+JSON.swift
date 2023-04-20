import struct Foundation.Data
import NIOCore

extension PostgresData {
    public init(json jsonData: Data) {
        let jsonData = [UInt8](jsonData)

        var buffer = ByteBufferAllocator()
            .buffer(capacity: jsonData.count)
        buffer.writeBytes(jsonData)
        self.init(type: .json, formatCode: .binary, value: buffer)
    }

    public init<T>(json value: T) throws where T: Encodable {
        let jsonData = try PostgresNIO._defaultJSONEncoder.encode(value)
        self.init(json: jsonData)
    }

    public var json: Data? {
        guard var value = self.value else {
            return nil
        }
        guard case .json = self.type else {
            return nil
        }
        guard let data = value.readBytes(length: value.readableBytes) else {
            return nil
        }
        return Data(data)
    }

    public func json<T>(as type: T.Type) throws -> T? where T: Decodable {
        guard let data = self.json else {
            return nil
        }
        return try PostgresNIO._defaultJSONDecoder.decode(T.self, from: data)
    }
}

@available(*, deprecated, message: "This protocol is going to be replaced with ``PostgresEncodable`` and ``PostgresDecodable`` and conforming to ``Codable`` at the same time")
public protocol PostgresJSONCodable: Codable, PostgresDataConvertible { }

@available(*, deprecated, message: "Deprecating conformance to `PostgresDataConvertible`, since it is deprecated.")
extension PostgresJSONCodable {
    public static var postgresDataType: PostgresDataType {
        return .json
    }

    public var postgresData: PostgresData? {
        return try? .init(json: self)
    }

    public init?(postgresData: PostgresData) {
        guard let value = try? postgresData.json(as: Self.self) else {
            return nil
        }
        self = value
    }
}
