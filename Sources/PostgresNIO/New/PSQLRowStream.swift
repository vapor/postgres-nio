import NIOCore
import Atomics
import Logging

struct QueryResult {
    enum Value: Equatable {
        case noRows(PSQLRowStream.StatementSummary)
        case rowDescription([RowDescription.Column])
    }

    var value: Value

    var logger: Logger
}

// Thread safety is guaranteed in the RowStream through dispatching onto the NIO EventLoop.
final class PSQLRowStream: Sendable {
    private typealias AsyncSequenceSource = NIOThrowingAsyncSequenceProducer<DataRow, any Error, AdaptiveRowBuffer, PSQLRowStream>.Source

    enum StatementSummary: Equatable {
        case tag(String)
        case emptyResponse
    }

    enum Source {
        case stream([RowDescription.Column], any PSQLRowsDataSource)
        case noRows(Result<StatementSummary, any Error>)
    }

    let eventLoop: any EventLoop
    let logger: Logger

    private enum BufferState {
        /// The query is still running and the result is being streamed as rows come in.
        case streaming(buffer: CircularBuffer<DataRow>, dataSource: any PSQLRowsDataSource)
        /// The query finished succesfully and the result is in the buffer.
        case finished(buffer: CircularBuffer<DataRow>, summary: StatementSummary)
        /// The query failed and consuming the row will yield the error.
        case failure(any Error)
    }

    /// The state the consumer is in.
    private enum DownstreamState {
        /// There is no consumer yet, we are waiting for one and we carry the state of the result buffer along.
        case waitingForConsumer(BufferState)
        /// Currently streaming rows on the EventLoop from the data source and calling `onRow` on each.
        case iteratingRows(onRow: (PostgresRow) throws -> (), EventLoopPromise<Void>, any PSQLRowsDataSource)
        /// Waiting to buffer all rows from the query.
        case waitingForAll([PostgresRow], EventLoopPromise<[PostgresRow]>, any PSQLRowsDataSource)
        /// An AsyncSequence is being used to consume the data, calling onFinish on end.
        case asyncSequence(AsyncSequenceSource, any PSQLRowsDataSource, onFinish: @Sendable () -> ())
        /// The consumer is done consuming the result, either with a summary or with an error.
        case consumed(Result<StatementSummary, any Error>)
        /// The stream was invalidated from us, currently this only happens if the stream was used outside 
        /// of the structured query API closure.
        case invalidated(any Error)
    }

    internal let rowDescription: [RowDescription.Column]
    private let lookupTable: [String: Int]
    private let downstreamStateBox: NIOLoopBoundBox<DownstreamState>
    /// The number of rows the server has sent for this query so far. Used for tracing.
    let receivedRowCount = ManagedAtomic(0)

    init(
        source: Source,
        eventLoop: any EventLoop,
        logger: Logger
    ) {
        let bufferState: BufferState
        switch source {
        case .stream(let rowDescription, let dataSource):
            self.rowDescription = rowDescription
            bufferState = .streaming(buffer: .init(), dataSource: dataSource)
        case .noRows(.success(let summary)):
            self.rowDescription = []
            bufferState = .finished(buffer: .init(), summary: summary)
        case .noRows(.failure(let error)):
            self.rowDescription = []
            bufferState = .failure(error)
        }

        self.eventLoop = eventLoop
        self.downstreamStateBox = NIOLoopBoundBox.init(.waitingForConsumer(bufferState), eventLoop: eventLoop)

        self.logger = logger

        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }

    // Every state modification below returns the side effects it wants to have run as an action. The
    // actions are then run *after* the modifying closure has returned. This is important, since all
    // side effects (yielding into the async sequence, requesting more rows from the data source,
    // succeeding promises, ...) may reenter this type. Running them while we hold exclusive access to
    // the downstream state would violate the exclusivity of that access.

    // MARK: Async Sequence

    private enum AsyncSequenceAction {
        /// Yield the buffered rows into the source and request more, if the source asks for more.
        case yieldAndRequestMore(CircularBuffer<DataRow>, any PSQLRowsDataSource)
        /// Yield the buffered rows into the source, finish it and invoke `onFinish`.
        case yieldAndFinish(CircularBuffer<DataRow>)
        /// Fail the source. The stream never had a data source, so there is nothing to cancel.
        case fail(any Error)
    }

    func asyncSequence(onFinish: @escaping @Sendable () -> () = {}) -> PostgresRowSequence {
        let producer = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: DataRow.self,
            failureType: (any Error).self,
            backPressureStrategy: AdaptiveRowBuffer(),
            finishOnDeinit: false,
            delegate: self
        )
        let source = producer.source

        let action = self.downstreamStateBox.withValue { state -> AsyncSequenceAction in
            let bufferState: BufferState
            switch state {
            case .waitingForConsumer(let bState): 
                bufferState = bState
            case .invalidated(let error):
                return .fail(error)
            case .asyncSequence, .consumed, .iteratingRows, .waitingForAll: 
                preconditionFailure("Invalid state: \(state)")
            }

            switch bufferState {
            case .streaming(let bufferedRows, let dataSource):
                state = .asyncSequence(source, dataSource, onFinish: onFinish)
                return .yieldAndRequestMore(bufferedRows, dataSource)

            case .finished(let buffer, let summary):
                state = .consumed(.success(summary))
                return .yieldAndFinish(buffer)

            case .failure(let error):
                state = .consumed(.failure(error))
                return .fail(error)
            }
        }

        switch action {
        case .yieldAndRequestMore(let rows, let dataSource):
            self.yield(rows, into: source, requestingMoreFrom: dataSource)

        case .yieldAndFinish(let rows):
            _ = source.yield(contentsOf: rows)
            source.finish()
            onFinish()

        case .fail(let error):
            source.finish(error)
        }

        return PostgresRowSequence(producer.sequence, lookupTable: self.lookupTable, columns: self.rowDescription)
    }

    /// Disallows further consumption of the stream by throwing an error on the next consumption attempt.
    /// Currently this is only used to disallow consuming the stream used outside of the structured query API closure.
    /// The buffer is drained before throwing, so if we have rows queued they will still be yielded before throwing the error.
    func invalidate(error: any Error) {
        if self.eventLoop.inEventLoop {
            self.invalidate0(error: error)
        } else {
            self.eventLoop.execute {
                self.invalidate0(error: error)
            }
        }
    }

    private enum InvalidateAction {
        case none
        case cancelDataSource(any PSQLRowsDataSource)
        case cancelDataSourceFailingVoidPromise(any PSQLRowsDataSource, EventLoopPromise<Void>)
        case cancelDataSourceFailingRowsPromise(any PSQLRowsDataSource, EventLoopPromise<[PostgresRow]>)
        case cancelDataSourceAndFinishSequence(any PSQLRowsDataSource, AsyncSequenceSource, onFinish: @Sendable () -> ())
    }

    private func invalidate0(error: any Error) {
        let action = self.downstreamStateBox.withValue { state -> InvalidateAction in
            switch state {
            case .waitingForConsumer(let bufferState):
                state = .invalidated(error)

                switch bufferState {
                case .streaming(_, dataSource: let dataSource):
                    return .cancelDataSource(dataSource)
                case .failure, .finished: 
                    return .none
                }

            case .iteratingRows(_, let promise, let dataSource):
                state = .invalidated(error)
                return .cancelDataSourceFailingVoidPromise(dataSource, promise)

            case .waitingForAll(_, let promise, let dataSource):
                state = .invalidated(error)
                return .cancelDataSourceFailingRowsPromise(dataSource, promise)

            case .asyncSequence(let asyncSequenceSource, let dataSource, let onFinish):
                state = .invalidated(error)
                return .cancelDataSourceAndFinishSequence(dataSource, asyncSequenceSource, onFinish: onFinish)

            case .consumed, .invalidated:
                return .none
            }
        }

        switch action {
        case .none: 
            break

        case .cancelDataSource(let dataSource):
            dataSource.cancel(for: self)

        case .cancelDataSourceFailingVoidPromise(let dataSource, let promise):
            dataSource.cancel(for: self)
            promise.fail(error)

        case .cancelDataSourceFailingRowsPromise(let dataSource, let promise):
            dataSource.cancel(for: self)
            promise.fail(error)

        case .cancelDataSourceAndFinishSequence(let dataSource, let sequenceSource, let onFinish):
            dataSource.cancel(for: self)
            sequenceSource.finish(error)
            onFinish()
        }
    }

    func demand() {
        if self.eventLoop.inEventLoop {
            self.demand0()
        } else {
            self.eventLoop.execute {
                self.demand0()
            }
        }
    }

    private enum DemandAction {
        case none
        case requestMore(any PSQLRowsDataSource)
    }

    private func demand0() {
        let action = self.downstreamStateBox.withValue { state -> DemandAction in
            switch state {
            case .waitingForConsumer, .iteratingRows, .waitingForAll:
                preconditionFailure("Invalid state: \(state)")

            case .consumed, .invalidated:
                return .none

            case .asyncSequence(_, let dataSource, _):
                return .requestMore(dataSource)
            }
        }

        switch action {
        case .none:
            break
        case .requestMore(let dataSource):
            dataSource.request(for: self)
        }
    }

    func cancel() {
        if self.eventLoop.inEventLoop {
            self.cancel0()
        } else {
            self.eventLoop.execute {
                self.cancel0()
            }
        }
    }

    private enum CancelAction {
        case none
        case cancelDataSource(any PSQLRowsDataSource, onFinish: @Sendable () -> ())
    }

    private func cancel0() {
        let action = self.downstreamStateBox.withValue { state -> CancelAction in
            switch state {
            case .asyncSequence(_, let dataSource, let onFinish):
                state = .consumed(.failure(CancellationError()))
                return .cancelDataSource(dataSource, onFinish: onFinish)

            case .consumed, .invalidated:
                return .none

            case .waitingForConsumer, .iteratingRows, .waitingForAll:
                preconditionFailure("Invalid state: \(state)")
            }
        }

        switch action {
        case .none:
            break
        case .cancelDataSource(let dataSource, let onFinish):
            dataSource.cancel(for: self)
            onFinish()
        }
    }

    // MARK: Consume in array

    func all() -> EventLoopFuture<[PostgresRow]> {
        if self.eventLoop.inEventLoop {
            return self.all0()
        } else {
            return self.eventLoop.flatSubmit {
                self.all0()
            }
        }
    }

    private enum AllAction {
        /// More rows will follow. Ask the data source for them.
        case requestMore(any PSQLRowsDataSource)
        case succeedPromise([PostgresRow])
        case failPromise(any Error)
    }

    private func all0() -> EventLoopFuture<[PostgresRow]> {
        let promise = self.eventLoop.makePromise(of: [PostgresRow].self)

        let action = self.downstreamStateBox.withValue { state -> AllAction in
            let bufferState: BufferState
            switch state {
            case .waitingForConsumer(let bState): 
                bufferState = bState
            case .invalidated(let error):
                return .failPromise(error)
            case .asyncSequence, .consumed, .iteratingRows, .waitingForAll: 
                preconditionFailure("Invalid state: \(state)")
            }

            switch bufferState {
            case .streaming(let bufferedRows, let dataSource):
                state = .waitingForAll(self.makeRows(bufferedRows), promise, dataSource)
                return .requestMore(dataSource)

            case .finished(let buffer, let summary):
                state = .consumed(.success(summary))
                return .succeedPromise(self.makeRows(buffer))

            case .failure(let error):
                state = .consumed(.failure(error))
                return .failPromise(error)
            }
        }

        switch action {
        case .requestMore(let dataSource):
            dataSource.request(for: self)
        case .succeedPromise(let rows):
            promise.succeed(rows)
        case .failPromise(let error):
            promise.fail(error)
        }

        return promise.futureResult
    }

    // MARK: Consume on EventLoop

    func onRow(_ onRow: @Sendable @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        if self.eventLoop.inEventLoop {
            return self.onRow0(onRow)
        } else {
            return self.eventLoop.flatSubmit {
                self.onRow0(onRow)
            }
        }
    }

    private enum OnRowAction {
        /// Forward the buffered rows to `onRow`. More rows will follow.
        case forwardRows(CircularBuffer<DataRow>, any PSQLRowsDataSource)
        /// Forward the buffered rows to `onRow` and succeed the promise afterwards. No rows will follow.
        case forwardFinalRows(CircularBuffer<DataRow>)
        case failPromise(any Error)
    }

    private func onRow0(_ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)

        let action = self.downstreamStateBox.withValue { state -> OnRowAction in
            let bufferState: BufferState
            switch state {
            case .waitingForConsumer(let bState): 
                bufferState = bState
            case .invalidated(let error):
                return .failPromise(error)
            case .asyncSequence, .consumed, .iteratingRows, .waitingForAll: 
                preconditionFailure("Invalid state: \(state)")
            }

            switch bufferState {
            case .streaming(let buffer, let dataSource):
                state = .iteratingRows(onRow: onRow, promise, dataSource)
                return .forwardRows(buffer, dataSource)

            case .finished(let buffer, let summary):
                state = .consumed(.success(summary))
                return .forwardFinalRows(buffer)

            case .failure(let error):
                state = .consumed(.failure(error))
                return .failPromise(error)
            }
        }

        switch action {
        case .forwardRows(let rows, let dataSource):
            self.forward(rows, to: onRow, requestingMoreFrom: dataSource)

        case .forwardFinalRows(let rows):
            do {
                try self.forward(rows, to: onRow)
                promise.succeed(())
            } catch {
                // The state is `.consumed(.success)` at this point. Since the user's closure has
                // thrown, we must report the failure instead.
                self.downstreamStateBox.withValue { $0 = .consumed(.failure(error)) }
                promise.fail(error)
            }

        case .failPromise(let error):
            promise.fail(error)
        }

        return promise.futureResult
    }

    internal func noticeReceived(_ notice: PostgresBackendMessage.NoticeResponse) {
        self.logger.debug("Notice Received", metadata: [
            .notice: "\(notice)"
        ])
    }

    private enum ReceiveRowsAction {
        /// The rows have been buffered. Nothing to do.
        case none
        /// Forward the new rows to `onRow`. More rows may follow.
        case forwardRows((PostgresRow) throws -> (), any PSQLRowsDataSource)
        /// The rows have been buffered for a `.all()` consumer. Ask the data source for more.
        case requestMore(any PSQLRowsDataSource)
        /// Yield the new rows into the source and request more, if the source asks for more.
        case yieldAndRequestMore(AsyncSequenceSource, any PSQLRowsDataSource)
    }

    internal func receive(_ newRows: [DataRow]) {
        precondition(!newRows.isEmpty, "Expected to get rows!")
        self.logger.trace("Row stream received rows", metadata: [
            "row_count": "\(newRows.count)"
        ])
        self.receivedRowCount.wrappingIncrement(by: newRows.count, ordering: .relaxed)

        let action = self.downstreamStateBox.withValue { state -> ReceiveRowsAction in
            switch state {
            case .waitingForConsumer(.streaming(buffer: var buffer, dataSource: let dataSource)):
                buffer.append(contentsOf: newRows)
                state = .waitingForConsumer(.streaming(buffer: buffer, dataSource: dataSource))
                return .none

            case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
                preconditionFailure("How can new rows be received, if an end was already signalled?")

            case .iteratingRows(let onRow, _, let dataSource):
                return .forwardRows(onRow, dataSource)

            case .waitingForAll(var rows, let promise, let dataSource):
                rows.append(contentsOf: self.makeRows(newRows))
                state = .waitingForAll(rows, promise, dataSource)
                return .requestMore(dataSource)

            case .asyncSequence(let source, let dataSource, _):
                return .yieldAndRequestMore(source, dataSource)

            case .consumed(.success):
                preconditionFailure("How can we receive further rows, if we are supposed to be done")

            case .consumed(.failure), .invalidated:
                return .none
            }
        }

        switch action {
        case .none:
            break

        case .forwardRows(let onRow, let dataSource):
            self.forward(newRows, to: onRow, requestingMoreFrom: dataSource)

        case .requestMore(let dataSource):
            dataSource.request(for: self)

        case .yieldAndRequestMore(let source, let dataSource):
            self.yield(newRows, into: source, requestingMoreFrom: dataSource)
        }
    }

    internal func receive(completion result: Result<String, any Error>) {
        switch result {
        case .success(let commandTag):
            self.receiveEnd(commandTag)
        case .failure(let error):
            self.receiveError(error)
        }
    }

    private enum ReceiveEndAction {
        case none
        case succeedVoidPromise(EventLoopPromise<Void>)
        case succeedRowsPromise(EventLoopPromise<[PostgresRow]>, [PostgresRow])
        case finishAsyncSequence(AsyncSequenceSource, onFinish: @Sendable () -> ())
    }

    private func receiveEnd(_ commandTag: String) {
        let action = self.downstreamStateBox.withValue { state -> ReceiveEndAction in
            switch state {
            case .waitingForConsumer(.streaming(buffer: let buffer, _)):
                state = .waitingForConsumer(.finished(buffer: buffer, summary: .tag(commandTag)))
                return .none

            case .waitingForConsumer(.finished), .waitingForConsumer(.failure), .consumed(.success(.emptyResponse)):
                preconditionFailure("How can we get another end, if an end was already signalled?")

            case .iteratingRows(_, let promise, _):
                state = .consumed(.success(.tag(commandTag)))
                return .succeedVoidPromise(promise)

            case .waitingForAll(let rows, let promise, _):
                state = .consumed(.success(.tag(commandTag)))
                return .succeedRowsPromise(promise, rows)

            case .asyncSequence(let source, _, let onFinish):
                state = .consumed(.success(.tag(commandTag)))
                return .finishAsyncSequence(source, onFinish: onFinish)

            case .consumed(.success(.tag)), .consumed(.failure), .invalidated:
                return .none
            }
        }

        switch action {
        case .none:
            break
        case .succeedVoidPromise(let promise):
            promise.succeed(())
        case .succeedRowsPromise(let promise, let rows):
            promise.succeed(rows)
        case .finishAsyncSequence(let source, let onFinish):
            source.finish()
            onFinish()
        }
    }

    private enum ReceiveErrorAction {
        case none
        case failVoidPromise(EventLoopPromise<Void>)
        case failRowsPromise(EventLoopPromise<[PostgresRow]>)
        case failAsyncSequence(AsyncSequenceSource, onFinish: @Sendable () -> ())
    }

    private func receiveError(_ error: any Error) {
        let action = self.downstreamStateBox.withValue { state -> ReceiveErrorAction in
            switch state {
            case .waitingForConsumer(.streaming):
                state = .waitingForConsumer(.failure(error))
                return .none

            case .waitingForConsumer(.finished), .waitingForConsumer(.failure), .consumed(.success(.emptyResponse)):
                preconditionFailure("How can we get another end, if an end was already signalled?")

            case .iteratingRows(_, let promise, _):
                state = .consumed(.failure(error))
                return .failVoidPromise(promise)

            case .waitingForAll(_, let promise, _):
                state = .consumed(.failure(error))
                return .failRowsPromise(promise)

            case .asyncSequence(let source, _, let onFinish):
                state = .consumed(.failure(error))
                return .failAsyncSequence(source, onFinish: onFinish)

            case .consumed(.success(.tag)), .consumed(.failure), .invalidated:
                return .none
            }
        }

        switch action {
        case .none:
            break
        case .failVoidPromise(let promise):
            promise.fail(error)
        case .failRowsPromise(let promise):
            promise.fail(error)
        case .failAsyncSequence(let source, let onFinish):
            source.finish(error)
            onFinish()
        }
    }

    // MARK: Side effects

    private func makeRows(_ dataRows: some Sequence<DataRow>) -> [PostgresRow] {
        dataRows.map {
            PostgresRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription)
        }
    }

    private func yield(
        _ newRows: some Sequence<DataRow>,
        into source: AsyncSequenceSource,
        requestingMoreFrom dataSource: any PSQLRowsDataSource
    ) {
        self.eventLoop.preconditionInEventLoop()

        switch source.yield(contentsOf: newRows) {
        case .dropped, .stopProducing:
            break
        case .produceMore:
            dataSource.request(for: self)
        }
    }

    private func forward(_ newRows: some Sequence<DataRow>, to onRow: (PostgresRow) throws -> ()) throws {
        for data in newRows {
            try onRow(PostgresRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription))
        }
    }

    private func forward(
        _ newRows: some Sequence<DataRow>,
        to onRow: (PostgresRow) throws -> (),
        requestingMoreFrom dataSource: any PSQLRowsDataSource
    ) {
        self.eventLoop.preconditionInEventLoop()

        do {
            try self.forward(newRows, to: onRow)
        } catch {
            return self.forwardingFailed(error)
        }
        // immediately request more
        dataSource.request(for: self)
    }

    private enum ForwardingFailedAction {
        case none
        case cancelDataSourceAndFailPromise(any PSQLRowsDataSource, EventLoopPromise<Void>)
    }

    /// Called if the user supplied `onRow` closure has thrown while we were forwarding rows to it.
    private func forwardingFailed(_ error: any Error) {
        let action = self.downstreamStateBox.withValue { state -> ForwardingFailedAction in
            switch state {
            case .iteratingRows(_, let promise, let dataSource):
                state = .consumed(.failure(error))
                return .cancelDataSourceAndFailPromise(dataSource, promise)

            case .consumed:
                return .none

            case .waitingForConsumer, .waitingForAll, .asyncSequence, .invalidated:
                preconditionFailure("Invalid state: \(state)")
            }
        }

        switch action {
        case .none:
            break
        case .cancelDataSourceAndFailPromise(let dataSource, let promise):
            dataSource.cancel(for: self)
            promise.fail(error)
        }
    }

    var commandTag: String {
        let consumed = self.downstreamStateBox.withValue { state in
            guard case .consumed(.success(let consumed)) = state else {
                preconditionFailure("commandTag may only be called if all rows have been consumed")
            }
            return consumed
        }
        return Self.commandTag(for: consumed)
    }

    /// The command tag of the finished query, or `nil` if the consumer has not run the stream to its end
    /// (still streaming, never consumed, or invalidated). If the stream finished with an error, the returned
    /// future fails with that error.
    func consumedCommandTag() -> EventLoopFuture<String?> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeCompletedFuture(self.consumedCommandTag0())
        } else {
            return self.eventLoop.flatSubmit {
                self.eventLoop.makeCompletedFuture(self.consumedCommandTag0())
            }
        }
    }

    private func consumedCommandTag0() -> Result<String?, any Error> {
        self.downstreamStateBox.withValue { state in
            switch state {
            case .consumed(.success(let summary)):
                return .success(Self.commandTag(for: summary))

            case .consumed(.failure(is CancellationError)):
                // The consumer dropped the iterator before the end, so there is no tag to report.
                return .success(nil)

            case .consumed(.failure(let error)):
                return .failure(error)
            
            case .waitingForConsumer, .iteratingRows, .waitingForAll, .asyncSequence, .invalidated:
                return .success(nil)
            }
        }
    }

    private static func commandTag(for summary: StatementSummary) -> String {
        switch summary {
        case .tag(let tag):
            return tag
        case .emptyResponse:
            return ""
        }
    }
}

extension PSQLRowStream: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        self.demand()
    }

    func didTerminate() {
        self.cancel()
    }
}

protocol PSQLRowsDataSource {

    func request(for stream: PSQLRowStream)
    func cancel(for stream: PSQLRowStream)

}
