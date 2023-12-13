import Foundation
import NIOCore

extension PostgresData {
    public init(uuid: UUID) {
        var buffer = ByteBufferAllocator().buffer(capacity: 16)
        buffer.writeUUIDBytes(uuid)
        self.init(type: .uuid, formatCode: .binary, value: buffer)
    }
    
    public var uuid: UUID? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.formatCode {
        case .binary:
            switch self.type {
            case .uuid:
                return value.readUUIDBytes()
            case .varchar, .text:
                return self.string.flatMap { UUID(uuidString: $0) }
            default:
                return nil
            }
        case .text:
            return nil
        }
    }
}
