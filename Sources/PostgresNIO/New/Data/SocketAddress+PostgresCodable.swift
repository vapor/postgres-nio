import NIOCore

extension SocketAddress: PostgresDecodable {
    public init<JSONDecoder: PostgresJSONDecoder>(from byteBuffer: inout NIOCore.ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PostgresDecodingContext<JSONDecoder>) throws {
        // IP family
        byteBuffer.moveReaderIndex(forwardBy: MemoryLayout<UInt8>.size)

        // netmask length in bits
        guard let netmaskLength: UInt8 = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        // ensure it is not a CIDR
        guard byteBuffer.readInteger(as: UInt8.self) == 0 else {
            throw PostgresDecodingError.Code.failure
        }

        // address length in bytes
        guard let addressLength: UInt8 = byteBuffer.readInteger(as: UInt8.self),
            addressLength * 8 == netmaskLength,
            let packedIPAddress: ByteBuffer = byteBuffer.readSlice(length: Int(addressLength))
        else {
            throw PostgresDecodingError.Code.failure
        }

        try self.init(packedIPAddress: packedIPAddress, port: 0)
    }
}

extension SocketAddress: PostgresEncodable & PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType { return .inet }
    public static var psqlFormat: PostgresFormat { .binary }
    public func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) {
        switch self {
        case .v4(let address):
            // IP family
            byteBuffer.writeInteger(UInt8(2))
            // netmask length in bits
            byteBuffer.writeInteger(UInt8(32))
            // indicate it is not a CIDR
            byteBuffer.writeInteger(UInt8(0))
            // address length in bytes
            byteBuffer.writeInteger(UInt8(4))
            // address values
            let addressBytes = withUnsafeBytes(of: address.address.sin_addr.s_addr) { Array($0) }
            byteBuffer.writeBytes(addressBytes)

        case .v6(let address):
            // IP family
            byteBuffer.writeInteger(UInt8(3))
            // netmask length in bits
            byteBuffer.writeInteger(UInt8(128))
            // indicate it is not a CIDR
            byteBuffer.writeInteger(UInt8(0))
            // address length in bytes
            byteBuffer.writeInteger(UInt8(16))
            // address values
            let addressBytes = withUnsafeBytes(of: address.address.sin6_addr) { Array($0) }
            byteBuffer.writeBytes(addressBytes)

        case .unixDomainSocket:
            fatalError("Cannot encode a UNIX socket address using the Postgres inet type")
        }
    }
}

extension SocketAddress: PostgresArrayDecodable {}

extension SocketAddress: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { return .inetArray }
}

extension SocketAddress: Decodable {
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let ipAddress = try container.decode(String.self)
        try self.init(ipAddress: ipAddress, port: 0)
    }
}

extension SocketAddress: Encodable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(self.description)
    }
}
