import NIOCore

extension PostgresMessage {
    /// Identifies the message as a notification response.
    public struct NotificationResponse: PostgresMessageType {
        public static let identifier = Identifier.notificationResponse

        /// Parses an instance of this message type from a byte buffer.
        public static func parse(from buffer: inout ByteBuffer) throws -> Self {
            guard let backendPID: Int32 = buffer.readInteger() else {
                throw PostgresError.protocol("Invalid NotificationResponse message: unable to read backend PID")
            }
            guard let channel = buffer.readNullTerminatedString() else {
                throw PostgresError.protocol("Invalid NotificationResponse message: unable to read channel")
            }
            guard let payload = buffer.readNullTerminatedString() else {
                throw PostgresError.protocol("Invalid NotificationResponse message: unable to read payload")
            }
            return .init(backendPID: backendPID, channel: channel, payload: payload)
        }

        public var backendPID: Int32
        public var channel: String
        public var payload: String
    }
}
