import Foundation

extension PostgresData {
    public init(uuid: UUID) {
        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        buffer.writeBytes([
            uuid.uuid.0, uuid.uuid.1, uuid.uuid.2, uuid.uuid.3,
            uuid.uuid.4, uuid.uuid.5, uuid.uuid.6, uuid.uuid.7,
            uuid.uuid.8, uuid.uuid.9, uuid.uuid.10, uuid.uuid.11,
            uuid.uuid.12, uuid.uuid.13, uuid.uuid.14, uuid.uuid.15,
        ])
        self.init(type: .uuid, formatCode: .binary, value: buffer)
    }
    
    public var uuid: UUID? {
        guard var value = self.value else {
            return nil
        }
        
        return value.readUUID()
    }
}
