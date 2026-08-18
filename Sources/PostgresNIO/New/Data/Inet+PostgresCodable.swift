import NIOCore

/// An IPv4 address within a ``PostgresInet``.
public struct PostgresIPv4 {
    /// The address's four octets.
    public let value: (UInt8, UInt8, UInt8, UInt8)

    public init(value: (UInt8, UInt8, UInt8, UInt8)) {
        self.value = value
    }
}

/// An IPv6 address within a ``PostgresInet``.
public struct PostgresIPv6 {
    /// The address's eight 16-bit groups.
    public let value: (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16)

    public init(value: (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16)) {
        self.value = value
    }
}

/// A Postgres `inet` value: an IPv4 or IPv6 address with an optional network mask.
public enum PostgresInet {
    case ipv4(PostgresIPv4, networkMask: UInt8?)
    case ipv6(PostgresIPv6, networkMask: UInt8?)
}

extension PostgresIPv4: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.value == rhs.value
    }
}

extension PostgresIPv6: Equatable {
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        lhs.value.0 == rhs.value.0
            && lhs.value.1 == rhs.value.1
            && lhs.value.2 == rhs.value.2
            && lhs.value.3 == rhs.value.3
            && lhs.value.4 == rhs.value.4
            && lhs.value.5 == rhs.value.5
            && lhs.value.6 == rhs.value.6
            && lhs.value.7 == rhs.value.7
    }
}

extension PostgresInet: Equatable {}

extension PostgresInet: PostgresDecodable {
    public init<JSONDecoder: PostgresJSONDecoder>(from byteBuffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PostgresDecodingContext<JSONDecoder>) throws {
        guard case .binary = format else {
            throw PostgresDecodingError.Code.failure
        }

        // IP family
        guard let ipFamily: UInt8 = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        // netmask length in bits
        guard let netmaskLength: UInt8 = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        // CIDR flag
        guard byteBuffer.readInteger(as: UInt8.self) == 0 else {
            throw PostgresDecodingError.Code.failure
        }

        // address length in bytes
        guard let addressLength = byteBuffer.readInteger(as: UInt8.self) else {
            throw PostgresDecodingError.Code.failure
        }

        if ipFamily == 2, addressLength == 4,
            let octet0 = byteBuffer.readInteger(as: UInt8.self),
            let octet1 = byteBuffer.readInteger(as: UInt8.self),
            let octet2 = byteBuffer.readInteger(as: UInt8.self),
            let octet3 = byteBuffer.readInteger(as: UInt8.self) {
            self = .ipv4(PostgresIPv4(value: (octet0, octet1, octet2, octet3)), networkMask: netmaskLength)
        } else if ipFamily == 3, addressLength == 16,
            let group0 = byteBuffer.readInteger(as: UInt16.self),
            let group1 = byteBuffer.readInteger(as: UInt16.self),
            let group2 = byteBuffer.readInteger(as: UInt16.self),
            let group3 = byteBuffer.readInteger(as: UInt16.self),
            let group4 = byteBuffer.readInteger(as: UInt16.self),
            let group5 = byteBuffer.readInteger(as: UInt16.self),
            let group6 = byteBuffer.readInteger(as: UInt16.self),
            let group7 = byteBuffer.readInteger(as: UInt16.self) {
            self = .ipv6(PostgresIPv6(value: (group0, group1, group2, group3, group4, group5, group6, group7)), networkMask: netmaskLength)
        } else {
            throw PostgresDecodingError.Code.failure
        }
    }
}

extension PostgresInet: PostgresEncodable & PostgresNonThrowingEncodable {
    public static var psqlType: PostgresDataType { .inet }
    public static var psqlFormat: PostgresFormat { .binary }
    public func encode<JSONEncoder: PostgresJSONEncoder>(into byteBuffer: inout ByteBuffer, context: PostgresEncodingContext<JSONEncoder>) {
        switch self {
        case .ipv4(let address, let networkMask):
            // IPv4 family
            byteBuffer.writeInteger(2, as: UInt8.self)
            byteBuffer.writeInteger(networkMask ?? 32, as: UInt8.self)
            // CIDR flag
            byteBuffer.writeInteger(0, as: UInt8.self)
            // address length in bytes
            byteBuffer.writeInteger(4, as: UInt8.self)
            byteBuffer.writeInteger(address.value.0, as: UInt8.self)
            byteBuffer.writeInteger(address.value.1, as: UInt8.self)
            byteBuffer.writeInteger(address.value.2, as: UInt8.self)
            byteBuffer.writeInteger(address.value.3, as: UInt8.self)
        case .ipv6(let address, let networkMask):
            // IPv6 family
            byteBuffer.writeInteger(3, as: UInt8.self)
            byteBuffer.writeInteger(networkMask ?? 128, as: UInt8.self)
            // CIDR flag
            byteBuffer.writeInteger(0, as: UInt8.self)
            // address length in bytes
            byteBuffer.writeInteger(16, as: UInt8.self)
            byteBuffer.writeInteger(address.value.0, as: UInt16.self)
            byteBuffer.writeInteger(address.value.1, as: UInt16.self)
            byteBuffer.writeInteger(address.value.2, as: UInt16.self)
            byteBuffer.writeInteger(address.value.3, as: UInt16.self)
            byteBuffer.writeInteger(address.value.4, as: UInt16.self)
            byteBuffer.writeInteger(address.value.5, as: UInt16.self)
            byteBuffer.writeInteger(address.value.6, as: UInt16.self)
            byteBuffer.writeInteger(address.value.7, as: UInt16.self)
        }
    }
}

extension PostgresInet: PostgresArrayDecodable {}

extension PostgresInet: PostgresArrayEncodable {
    public static var psqlArrayType: PostgresDataType { .inetArray }
}
