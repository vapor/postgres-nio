extension PostgresMessage {
    public struct Terminate: PostgresMessageType {
        public static var identifier: PostgresMessage.Identifier {
            .terminate
        }

        public func serialize(into buffer: inout ByteBuffer) { }
    }
}
