import NIOCore

struct BufferedMessageEncoder {
    private enum State {
        case flushed
        case writable
    }
    
    private var buffer: ByteBuffer
    private var state: State = .writable
    private var encoder: PSQLFrontendMessageEncoder
    
    init(buffer: ByteBuffer, encoder: PSQLFrontendMessageEncoder) {
        self.buffer = buffer
        self.encoder = encoder
    }
    
    mutating func encode(_ message: PSQLFrontendMessage) {
        switch self.state {
        case .flushed:
            self.state = .writable
            self.buffer.clear()
            
        case .writable:
            break
        }
        
        self.encoder.encode(data: message, out: &self.buffer)
    }
    
    mutating func flush() -> ByteBuffer {
        self.state = .flushed
        return self.buffer
    }
}
