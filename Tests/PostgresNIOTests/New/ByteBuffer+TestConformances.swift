import NIOCore

// These are conformances of ByteBuffer to the ExpressibleByLiteral protocols. Those make testing
// much nicer. **Never** move those into the library code!

extension ByteBuffer: ExpressibleByStringLiteral {
    public typealias StringLiteralType = String
    
    public init(stringLiteral: String) {
        self.init(string: stringLiteral)
    }
}
