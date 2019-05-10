extension PostgresData {
    public init(int value: Int) {
        self.init(fwi: value)
    }
    
    public init(int8 value: Int8) {
        self.init(fwi: value)
    }
    
    public init(int16 value: Int16) {
        self.init(fwi: value)
    }
    
    public init(int32 value: Int32) {
        self.init(fwi: value)
    }
    
    public init(int64 value: Int64) {
        self.init(fwi: value)
    }
    
    public init(uint value: UInt) {
        self.init(fwi: value)
    }
    
    public init(uint8 value: UInt8) {
        self.init(fwi: value)
    }
    
    public init(uint16 value: UInt16) {
        self.init(fwi: value)
    }
    
    public init(uint32 value: UInt32) {
        self.init(fwi: value)
    }
    
    public init(uint64 value: UInt64) {
        self.init(fwi: value)
    }
    
    public var int: Int? {
        return fwi()
    }
    
    public var int8: Int8? {
        return fwi()
    }
    
    public var int16: Int16? {
        return fwi()
    }
    
    public var int32: Int32? {
        return fwi()
    }
    
    public var int64: Int64? {
        return fwi()
    }
    
    public var uint: UInt? {
        return fwi()
    }
    
    public var uint8: UInt8? {
        return fwi()
    }
    
    public var uint16: UInt16? {
        return fwi()
    }
    
    public var uint32: UInt32? {
        return fwi()
    }
    
    public var uint64: UInt64? {
        return fwi()
    }
}

private extension PostgresData {
    init<I>(fwi: I) where I: FixedWidthInteger {
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
        var buffer = ByteBufferAllocator().buffer(capacity: capacity)
        buffer.writeInteger(fwi)
        self.init(type: type, formatCode: .binary, value: buffer)
    }
    
    func fwi<I>(_ type: I.Type = I.self) -> I?
        where I: FixedWidthInteger
    {
        guard var value = self.value else {
            fatalError()
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .int2:
                assert(value.readableBytes == 2)
                guard let int16 = value.readInteger(as: Int16.self) else {
                    fatalError()
                }
                return I(int16)
            case .int4, .regproc:
                assert(value.readableBytes == 4)
                guard let int32 = value.getInteger(at: value.readerIndex, as: Int32.self) else {
                    fatalError()
                }
                return I(int32)
            case .oid:
                assert(value.readableBytes == 4)
                guard let uint32 = value.getInteger(at: value.readerIndex, as: UInt32.self) else {
                    fatalError()
                }
                return I(uint32)
            case .int8:
                assert(value.readableBytes == 8)
                guard let int64 = value.getInteger(at: value.readerIndex, as: Int64.self) else {
                    fatalError()
                }
                return I(int64)
            default:
                return nil
            }
        case .text:
            guard let string = self.string else {
                return nil
            }
            return I(string)
        }
    }
    
}
