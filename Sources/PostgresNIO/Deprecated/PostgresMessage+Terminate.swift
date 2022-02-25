import NIOCore

extension PostgresMessage {
    @available(*, deprecated, message: "Will be removed from public API")
    public struct Terminate: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            .terminate
        }

        public func serialize(into buffer: inout ByteBuffer) { }
    }
}
