import NIO

extension Int: PostgresDataConvertible {}
extension Int8: PostgresDataConvertible {}
extension Int16: PostgresDataConvertible {}
extension Int32: PostgresDataConvertible {}
extension Int64: PostgresDataConvertible {}

extension UInt: PostgresDataConvertible {}
extension UInt8: PostgresDataConvertible {}
extension UInt16: PostgresDataConvertible {}
extension UInt32: PostgresDataConvertible {}
extension UInt64: PostgresDataConvertible {}

extension FixedWidthInteger {
    public init?(postgresData: PostgresData) {
        guard let value = postgresData.value else {
            return nil
        }
        
        switch postgresData.formatCode {
        case .binary:
            switch postgresData.type {
            case .int2:
                assert(value.readableBytes == 2)
                guard let int16 = value.getInteger(at: value.readerIndex, as: Int16.self) else {
                    return nil
                }
                self = Self(int16)
            case .int4, .regproc:
                assert(value.readableBytes == 4)
                guard let int32 = value.getInteger(at: value.readerIndex, as: Int32.self) else {
                    return nil
                }
                self = Self(int32)
            case .oid:
                assert(value.readableBytes == 4)
                guard let uint32 = value.getInteger(at: value.readerIndex, as: UInt32.self) else {
                    return nil
                }
                self = Self(uint32)
            case .int8:
                assert(value.readableBytes == 8)
                guard let int64 = value.getInteger(at: value.readerIndex, as: Int64.self) else {
                    return nil
                }
                self = Self(int64)
            case .numeric:
                #warning("Use numeric converter")
                fatalError("use numeric converter")
            default: fatalError("Cannot decode \(Self.self) from \(postgresData.type)")
            }
        case .text:
            guard let string = String(postgresData: postgresData) else {
                return nil
            }
            guard let converted = Self(string) else {
                return nil
            }
            self = converted
        }
    }
    
    public var postgresData: PostgresData? {
        let capacity: Int
        let type: PostgresDataType
        switch Self.bitWidth {
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
            fatalError("Cannot encode \(Self.self) to PostgresData")
        }
        #warning("should not be creating an allocator here")
        var buffer = ByteBufferAllocator.init().buffer(capacity: capacity)
        buffer.write(integer: self)
        return PostgresData(type: type, formatCode: .binary, value: buffer)
    }
}
