extension PostgresData {
    public init(int value: Int) {
        self.init(type: .int8, value: .init(integer: Int64(value)))
    }

    public init(uint8 value: UInt8) {
        self.init(type: .char, value: .init(integer: value))
    }
    
    public init(int16 value: Int16) {
        self.init(type: .int2, value: .init(integer: value))
    }
    
    public init(int32 value: Int32) {
        self.init(type: .int4, value: .init(integer: value))
    }
    
    public init(int64 value: Int64) {
        self.init(type: .int8, value: .init(integer: value))
    }
    
    public var int: Int? {
        guard var value = self.value else {
            return nil
        }

        switch self.formatCode {
        case .binary:
            switch self.type {
            case .char, .bpchar:
                guard value.readableBytes == 1 else {
                    return nil
                }
                return value.readInteger(as: UInt8.self).flatMap(Int.init)
            case .int2:
                assert(value.readableBytes == 2)
                return value.readInteger(as: Int16.self).flatMap(Int.init)
            case .int4, .regproc:
                assert(value.readableBytes == 4)
                return value.readInteger(as: Int32.self).flatMap(Int.init)
            case .oid:
                assert(value.readableBytes == 4)
                return value.readInteger(as: UInt32.self).flatMap { Int(exactly: $0) }
            case .int8:
                assert(value.readableBytes == 8)
                return value.readInteger(as: Int64.self).flatMap { Int(exactly: $0) }
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Int(string)
        }
    }

    public var uint8: UInt8? {
        guard var value = self.value else {
            return nil
        }

        switch self.formatCode {
        case .binary:
            switch self.type {
            case .char, .bpchar:
                guard value.readableBytes == 1 else {
                    return nil
                }
                return value.readInteger(as: UInt8.self)
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return UInt8(string)
        }
    }
    
    public var int16: Int16? {
        guard var value = self.value else {
            return nil
        }

        switch self.formatCode {
        case .binary:
            switch self.type {
            case .char, .bpchar:
                guard value.readableBytes == 1 else {
                    return nil
                }
                return value.readInteger(as: UInt8.self)
                    .flatMap(Int16.init)
            case .int2:
                assert(value.readableBytes == 2)
                return value.readInteger(as: Int16.self)
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Int16(string)
        }
    }
    
    public var int32: Int32? {
        guard var value = self.value else {
            return nil
        }

        switch self.formatCode {
        case .binary:
            switch self.type {
            case .char, .bpchar:
                guard value.readableBytes == 1 else {
                    return nil
                }
                return value.readInteger(as: UInt8.self)
                    .flatMap(Int32.init)
            case .int2:
                assert(value.readableBytes == 2)
                return value.readInteger(as: Int16.self)
                    .flatMap(Int32.init)
            case .int4, .regproc:
                assert(value.readableBytes == 4)
                return value.readInteger(as: Int32.self)
                    .flatMap(Int32.init)
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Int32(string)
        }
    }
    
    public var int64: Int64? {
        guard var value = self.value else {
            return nil
        }

        switch self.formatCode {
        case .binary:
            switch self.type {
            case .char, .bpchar:
                guard value.readableBytes == 1 else {
                    return nil
                }
                return value.readInteger(as: UInt8.self)
                    .flatMap(Int64.init)
            case .int2:
                assert(value.readableBytes == 2)
                return value.readInteger(as: Int16.self)
                    .flatMap(Int64.init)
            case .int4, .regproc:
                assert(value.readableBytes == 4)
                return value.readInteger(as: Int32.self)
                    .flatMap(Int64.init)
            case .oid:
                assert(value.readableBytes == 4)
                assert(Int.bitWidth == 64) // or else overflow is possible
                return value.readInteger(as: UInt32.self)
                    .flatMap(Int64.init)
            case .int8:
                assert(value.readableBytes == 8)
                assert(Int.bitWidth == 64)
                return value.readInteger(as: Int64.self)
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return Int64(string)
        }
    }
}

extension Int: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType { .int8 }

    public init?(postgresData: PostgresData) {
        guard let int = postgresData.int else {
            return nil
        }
        self = int
    }

    public var postgresData: PostgresData? {
        .init(int: self)
    }
}

extension UInt8: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType { .char }

    public init?(postgresData: PostgresData) {
        guard let uint8 = postgresData.uint8 else {
            return nil
        }
        self = uint8
    }

    public var postgresData: PostgresData? {
        .init(uint8: self)
    }
}

extension Int16: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType { .int2 }

    public init?(postgresData: PostgresData) {
        guard let int16 = postgresData.int16 else {
            return nil
        }
        self = int16
    }

    public var postgresData: PostgresData? {
        .init(int16: self)
    }
}

extension Int32: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType { .int4 }

    public init?(postgresData: PostgresData) {
        guard let int32 = postgresData.int32 else {
            return nil
        }
        self = int32
    }

    public var postgresData: PostgresData? {
        .init(int32: self)
    }
}

extension Int64: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType { .int8 }

    public init?(postgresData: PostgresData) {
        guard let int64 = postgresData.int64 else {
            return nil
        }
        self = int64
    }

    public var postgresData: PostgresData? {
        .init(int64: self)
    }
}
