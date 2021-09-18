import NIOCore

/// A sub state for receiving data rows. Stores whether the consumer has either signaled demand and whether the
/// channel has issued `read` events.
///
/// This should be used as a SubStateMachine in QuerySubStateMachines.
struct RowStreamStateMachine {
    
    enum Action {
        case read
        case wait
    }
    
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

    private var state: State

    init() {
        self.state = .waitingForRows(CircularBuffer(initialCapacity: 32))
    }

    mutating func receivedRow(_ newRow: PSQLBackendMessage.DataRow) {
        switch self.state {
        case .waitingForRows(var buffer):
            self.state = .modifying
            buffer.append(newRow)
            self.state = .waitingForRows(buffer)
            
        // For all the following cases, please note:
        // Normally these code paths should never be hit. However there is one way to trigger
        // this:
        //
        // If the server decides to close a connection, NIO will forward all outstanding
        // `channelRead`s without waiting for a next `context.read` call. For this reason we might
        // receive new rows, when we don't expect them here.
        case .waitingForRead(var buffer):
            self.state = .modifying
            buffer.append(newRow)
            self.state = .waitingForRead(buffer)
            
        case .waitingForDemand(var buffer):
            self.state = .modifying
            buffer.append(newRow)
            self.state = .waitingForDemand(buffer)
            
        case .waitingForReadOrDemand(var buffer):
            self.state = .modifying
            buffer.append(newRow)
            self.state = .waitingForReadOrDemand(buffer)

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
            // If we are `.waitingForRows`, no action needs to be taken. As soon as we receive
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

        case .waitingForReadOrDemand(let buffer),
             .waitingForRead(let buffer),
             .waitingForDemand(let buffer):
            
            // Normally this code path should never be hit. However there is one way to trigger
            // this:
            //
            // If the server decides to close a connection, NIO will forward all outstanding
            // `channelRead`s without waiting for a next `context.read` call. For this reason we might
            // receive a call to `end()`, when we don't expect it here.
            return buffer

        case .modifying:
            preconditionFailure("Invalid state: \(self.state)")
        }
    }
}
