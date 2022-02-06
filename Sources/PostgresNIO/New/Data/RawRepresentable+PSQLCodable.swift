import NIOCore

extension PSQLCodable where Self: RawRepresentable, RawValue: PSQLCodable {
    var psqlType: PSQLDataType {
        self.rawValue.psqlType
    }
    
    var psqlFormat: PSQLFormat {
        self.rawValue.psqlFormat
    }
    
    static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> Self {
        guard let rawValue = try? RawValue.decode(from: &buffer, type: type, format: format, context: context),
              let selfValue = Self.init(rawValue: rawValue) else {
            throw PSQLCastingError.Code.failure
        }
        
        return selfValue
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) throws {
        try rawValue.encode(into: &buffer, context: context)
    }
}
