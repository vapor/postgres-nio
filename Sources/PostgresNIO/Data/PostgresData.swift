import NIO

public struct PostgresData: CustomStringConvertible, CustomDebugStringConvertible {
    public static var null: PostgresData {
        return .init(type: .null)
    }
    
    /// The object ID of the field's data type.
    public var type: PostgresDataType
    
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    public var typeModifier: Int32?
    
    /// The format code being used for the field.
    /// Currently will be zero (text) or one (binary).
    /// In a RowDescription returned from the statement variant of Describe,
    /// the format code is not yet known and will always be zero.
    public var formatCode: PostgresFormatCode
    
    public var value: ByteBuffer?
    
    public init(type: PostgresDataType, typeModifier: Int32? = nil, formatCode: PostgresFormatCode = .binary, value: ByteBuffer? = nil) {
        self.type = type
        self.typeModifier = typeModifier
        self.formatCode = formatCode
        self.value = value
    }
    
    public var description: String {
        if let string = self.string {
            return string
        } else {
            let string: String
            if var value = self.value {
                switch self.formatCode {
                case .text:
                    let raw = value.readString(length: value.readableBytes) ?? ""
                    string = "\"\(raw)\""
                case .binary:
                    string = "0x" + value.readableBytesView.hexdigest()
                }
            } else {
                string = "<null>"
            }
            return string + " (\(self.type))"
        }
    }

    public var debugDescription: String {
        let valueDescription: String
        if let value = self.value {
            valueDescription = "\(value.readableBytes.description) bytes"
        } else {
            valueDescription = "nil"
        }
        return "PostgresData(type: \(self.type), value: \(valueDescription))"
    }
}
