import NIOCore

/// A sub state for receiving data rows. Stores whether the consumer has either signaled demand and whether the
/// channel has issued `read` events.
struct RowStreamStateMachine {
    private enum State {
        /// The state machines expects further writes to `channelRead`. The writes are appended to the buffer.
        case waitingForRows(CircularBuffer<PSQLBackendMessage.DataRow>)
        /// The state machines expects a call to `demandMoreResponseBodyParts` or `read`. The buffer is
        /// empty. It is preserved for performance reasons.
        case waitingForReadOrDemand(CircularBuffer<PSQLBackendMessage.DataRow>)
        /// The state machines expects a call to `read`. The buffer is empty. It is preserved for performance reasons.
        case waitingForRead(CircularBuffer<PSQLBackendMessage.DataRow>)
        /// The state machines expects a call to `demandMoreResponseBodyParts`. The buffer is empty. It is
        /// preserved for performance reasons.
        case waitingForDemand(CircularBuffer<PSQLBackendMessage.DataRow>)

        case modifying
    }

    enum Action {
        case read
        case wait
    }

    private var state: State

    init() {
        self.state = .waitingForRows(CircularBuffer(initialCapacity: 32))
    }

    mutating func receivedRows(_ newRows: [PSQLBackendMessage.DataRow]) {
        switch self.state {
        case .waitingForRows(var buffer):
            self.state = .modifying
            buffer.append(contentsOf: newRows)
            self.state = .waitingForRows(buffer)

        case .waitingForRead,
             .waitingForDemand,
             .waitingForReadOrDemand:
            preconditionFailure("How can we receive a body part, after a channelReadComplete, but no read has been forwarded yet. Invalid state: \(self.state)")

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }

    mutating func channelReadComplete() -> CircularBuffer<PSQLBackendMessage.DataRow>? {
        switch self.state {
        case .waitingForRows(let buffer):
            if buffer.isEmpty {
                self.state = .waitingForRead(buffer)
                return nil
            } else {
                var newBuffer = buffer
                newBuffer.removeAll(keepingCapacity: true)
                self.state = .waitingForReadOrDemand(newBuffer)
                return buffer
            }

        case .waitingForRead,
             .waitingForDemand,
             .waitingForReadOrDemand:
            preconditionFailure("How can we receive a body part, after a channelReadComplete, but no read has been forwarded yet. Invalid state: \(self.state)")

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }

    mutating func demandMoreResponseBodyParts() -> Action {
        switch self.state {
        case .waitingForDemand(let buffer):
            self.state = .waitingForRows(buffer)
            return .read

        case .waitingForReadOrDemand(let buffer):
            self.state = .waitingForRead(buffer)
            return .wait

        case .waitingForRead:
            // If we are `.waitingForRead`, no action needs to be taken. Demand has already been
            // signaled. Once we receive the next `read`, we will forward it, right away
            return .wait

        case .waitingForRows:
            // If we are `.waitingForBytes`, no action needs to be taken. As soon as we receive
            // the next `channelReadComplete` we will forward all buffered data
            return .wait

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }

    mutating func read() -> Action {
        switch self.state {
        case .waitingForRows:
            // This should never happen. But we don't want to precondition this behavior. Let's just
            // pass the read event on
            return .read

        case .waitingForReadOrDemand(let buffer):
            self.state = .waitingForDemand(buffer)
            return .wait

        case .waitingForRead(let buffer):
            self.state = .waitingForRows(buffer)
            return .read

        case .waitingForDemand:
            // we have already received a read event. We will issue it as soon as we received demand
            // from the consumer
            return .wait

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }

    mutating func end() -> CircularBuffer<PSQLBackendMessage.DataRow> {
        switch self.state {
        case .waitingForRows(let buffer):
            return buffer

        case .waitingForReadOrDemand,
             .waitingForRead,
             .waitingForDemand:
            preconditionFailure("How can we receive a body end, after a channelReadComplete, but no read has been forwarded yet. Invalid state: \(self.state)")

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
}
