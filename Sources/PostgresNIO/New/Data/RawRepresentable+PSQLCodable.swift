import NIOCore

extension PSQLCodable where Self: RawRepresentable, RawValue: PSQLCodable {
    var psqlType: PostgresDataType {
        self.rawValue.psqlType
    }
    
    var psqlFormat: PostgresFormat {
        self.rawValue.psqlFormat
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PostgresDataType, format: PostgresFormat, context: PSQLDecodingContext) throws -> Self {
        guard let rawValue = try? RawValue.decode(from: &buffer, type: type, format: format, context: context),
              let selfValue = Self.init(rawValue: rawValue) else {
            throw PSQLCastingError.Code.failure
        }
        
        return selfValue
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        try rawValue.encode(into: &byteBuffer, context: context)
    }
}
