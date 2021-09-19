import NIOCore

struct BufferedMessageEncoder<Encoder: MessageToByteEncoder> {
    private enum State {
        case flushed
        case writable
    }
    
    private var buffer: ByteBuffer
    private var state: State = .writable
    private var encoder: Encoder
    
    init(buffer: ByteBuffer, encoder: Encoder) {
        self.buffer = buffer
        self.encoder = encoder
    }
    
    mutating func encode(_ message: Encoder.OutboundIn) throws {
        switch self.state {
        case .flushed:
            self.state = .writable
            self.buffer.clear()
            
        case .writable:
            break
        }
        
        try self.encoder.encode(data: message, out: &self.buffer)
    }
    
    mutating func flush() -> ByteBuffer? {
        guard self.buffer.readableBytes > 0 else {
            return nil
        }
        
        self.state = .flushed
        return self.buffer
    }
}
