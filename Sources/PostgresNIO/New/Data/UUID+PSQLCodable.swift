import NIOCore
import struct Foundation.UUID
import typealias Foundation.uuid_t

extension UUID: PSQLCodable {
    
    public var psqlType: PSQLDataType {
        .uuid
    }
    
    public var psqlFormat: PSQLFormat {
        .binary
    }
    
    public func encode<JSONEncoder: PSQLJSONEncoder>(into buffer: inout ByteBuffer, context: PSQLEncodingContext<JSONEncoder>) {
        let uuid = self.uuid
        buffer.writeBytes([
            uuid.0, uuid.1, uuid.2, uuid.3,
            uuid.4, uuid.5, uuid.6, uuid.7,
            uuid.8, uuid.9, uuid.10, uuid.11,
            uuid.12, uuid.13, uuid.14, uuid.15,
        ])
    }

    @inlinable
    public static func decode<JSONDecoder : PSQLJSONDecoder>(
        from buffer: inout ByteBuffer,
        type: PSQLDataType,
        format: PSQLFormat,
        context: PSQLDecodingContext<JSONDecoder>
    ) throws -> UUID {
        switch (format, type) {
        case (.binary, .uuid):
            guard let uuid = buffer.readUUID() else {
                throw PSQLCastingError.Code.failure
            }
            return uuid
        case (.binary, .varchar),
             (.binary, .text),
             (.text, .uuid),
             (.text, .text),
             (.text, .varchar):
            guard buffer.readableBytes == 36 else {
                throw PSQLCastingError.Code.failure
            }
            
            guard let uuid = buffer.readString(length: 36).flatMap({ UUID(uuidString: $0) }) else {
                throw PSQLCastingError.Code.failure
            }
            return uuid
        default:
            throw PSQLCastingError.Code.typeMismatch
        }
    }
}

extension ByteBuffer {
    @usableFromInline
    mutating func readUUID() -> UUID? {
        guard self.readableBytes >= MemoryLayout<uuid_t>.size else {
            return nil
        }
        
        let value: UUID = self.getUUID(at: self.readerIndex)! /* must work as we have enough bytes */
        // should be MoveReaderIndex
        self.moveReaderIndex(forwardBy: MemoryLayout<uuid_t>.size)
        return value
    }

    @usableFromInline
    func getUUID(at index: Int) -> UUID? {
        var uuid: uuid_t = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        return self.viewBytes(at: index, length: MemoryLayout.size(ofValue: uuid)).map { bufferBytes in
            withUnsafeMutableBytes(of: &uuid) { target in
                precondition(target.count <= bufferBytes.count)
                target.copyBytes(from: bufferBytes)
            }
            return UUID(uuid: uuid)
        }
    }
}
