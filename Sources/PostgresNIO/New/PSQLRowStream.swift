import Atomics
import NIOCore
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
final class PSQLRowStream: @unchecked Sendable {
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
        case streaming(buffer: CircularBuffer<DataRow>, dataSource: any PSQLRowsDataSource)
        case finished(buffer: CircularBuffer<DataRow>, summary: StatementSummary)
        case failure(any Error)
    }

    private enum DownstreamState {
        case waitingForConsumer(BufferState)
        case iteratingRows(onRow: (PostgresRow) throws -> (), EventLoopPromise<Void>, any PSQLRowsDataSource)
        case waitingForAll([PostgresRow], EventLoopPromise<[PostgresRow]>, any PSQLRowsDataSource)
        case consumed(Result<StatementSummary, any Error>)
        case asyncSequence(AsyncSequenceSource, any PSQLRowsDataSource, onFinish: @Sendable () -> ())
    }
    
    internal let rowDescription: [RowDescription.Column]
    private let lookupTable: [String: Int]
    private var downstreamState: DownstreamState
    private var tracing: (span: PostgresTraceSpan, managesLifecycle: Bool)?
    // Set to true (on the event loop) when CommandComplete arrives for the async sequence path.
    // Read from any thread in didTerminate() to end the span synchronously before the event loop hop.
    private let asyncSequenceCompletedSuccessfully = ManagedAtomic(false)
    
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
        
        self.downstreamState = .waitingForConsumer(bufferState)
        self.tracing = nil
        
        self.eventLoop = eventLoop
        self.logger = logger

        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }

    deinit {
        if let tracing = self.tracing, tracing.managesLifecycle {
            switch self.downstreamState {
            case .consumed(.success):
                tracing.span.succeed()
            default:
                tracing.span.fail(CancellationError())
            }
        }
    }

    func installTracing(_ span: PostgresTraceSpan, managesLifecycle: Bool) {
        self.eventLoop.preconditionInEventLoop()
        self.tracing = (span, managesLifecycle)
    }
    
    // MARK: Async Sequence

    func asyncSequence(onFinish: @escaping @Sendable () -> () = {}) -> PostgresRowSequence {
        self.eventLoop.preconditionInEventLoop()

        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        let producer = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: DataRow.self,
            failureType: (any Error).self,
            backPressureStrategy: AdaptiveRowBuffer(),
            finishOnDeinit: false,
            delegate: self
        )

        let source = producer.source
        
        switch bufferState {
        case .streaming(let bufferedRows, let dataSource):
            let yieldResult = source.yield(contentsOf: bufferedRows)
            self.downstreamState = .asyncSequence(source, dataSource, onFinish: onFinish)
            self.executeActionBasedOnYieldResult(yieldResult, source: dataSource)

        case .finished(let buffer, let summary):
            _ = source.yield(contentsOf: buffer)
            self.asyncSequenceCompletedSuccessfully.store(true, ordering: .relaxed)
            source.finish()
            onFinish()
            self.downstreamState = .consumed(.success(summary))

        case .failure(let error):
            source.finish(error)
            self.failTracingIfNeeded(error)
            self.downstreamState = .consumed(.failure(error))
        }
        
        return PostgresRowSequence(producer.sequence, lookupTable: self.lookupTable, columns: self.rowDescription)
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
    
    private func demand0() {
        switch self.downstreamState {
        case .waitingForConsumer, .iteratingRows, .waitingForAll:
            preconditionFailure("Invalid state: \(self.downstreamState)")
            
        case .consumed:
            break
            
        case .asyncSequence(_, let dataSource, _):
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

    private func cancel0() {
        switch self.downstreamState {
        case .asyncSequence(_, let dataSource, let onFinish):
            self.downstreamState = .consumed(.failure(CancellationError()))
            dataSource.cancel(for: self)
            self.failTracingIfNeeded(CancellationError())
            onFinish()

        case .consumed:
            return

        case .waitingForConsumer, .iteratingRows, .waitingForAll:
            preconditionFailure("Invalid state: \(self.downstreamState)")
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
    
    private func all0() -> EventLoopFuture<[PostgresRow]> {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        switch bufferState {
        case .streaming(let bufferedRows, let dataSource):
            let promise = self.eventLoop.makePromise(of: [PostgresRow].self)
            let rows = bufferedRows.map { data in
                PostgresRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription)
            }
            self.downstreamState = .waitingForAll(rows, promise, dataSource)
            // immediately request more
            dataSource.request(for: self)
            return promise.futureResult
            
        case .finished(let buffer, let summary):
            let rows = buffer.map {
                PostgresRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription)
            }
            
            self.downstreamState = .consumed(.success(summary))
            return self.eventLoop.makeSucceededFuture(rows)
            
        case .failure(let error):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
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
    
    private func onRow0(_ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        switch bufferState {
        case .streaming(var buffer, let dataSource):
            let promise = self.eventLoop.makePromise(of: Void.self)
            do {
                for data in buffer {
                    let row = PostgresRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try self.withTracingContext {
                        try onRow(row)
                    }
                }
                
                buffer.removeAll()
                self.downstreamState = .iteratingRows(onRow: onRow, promise, dataSource)
                // immediately request more
                dataSource.request(for: self)
            } catch {
                self.downstreamState = .consumed(.failure(error))
                dataSource.cancel(for: self)
                promise.fail(error)
            }
            
            return promise.futureResult

        case .finished(let buffer, let summary):
            do {
                for data in buffer {
                    let row = PostgresRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try self.withTracingContext {
                        try onRow(row)
                    }
                }
                
                self.downstreamState = .consumed(.success(summary))
                return self.eventLoop.makeSucceededVoidFuture()
            } catch {
                self.downstreamState = .consumed(.failure(error))
                return self.eventLoop.makeFailedFuture(error)
            }
            
        case .failure(let error):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    internal func noticeReceived(_ notice: PostgresBackendMessage.NoticeResponse) {
        self.logger.debug("Notice Received", metadata: [
            .notice: "\(notice)"
        ])
    }
    
    internal func receive(_ newRows: [DataRow]) {
        precondition(!newRows.isEmpty, "Expected to get rows!")
        self.eventLoop.preconditionInEventLoop()
        self.logger.trace("Row stream received rows", metadata: [
            "row_count": "\(newRows.count)"
        ])
        
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(buffer: var buffer, dataSource: let dataSource)):
            buffer.append(contentsOf: newRows)
            self.downstreamState = .waitingForConsumer(.streaming(buffer: buffer, dataSource: dataSource))
            
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("How can new rows be received, if an end was already signalled?")

        case .iteratingRows(let onRow, let promise, let dataSource):
            do {
                for data in newRows {
                    let row = PostgresRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try self.withTracingContext {
                        try onRow(row)
                    }
                }
                // immediately request more
                dataSource.request(for: self)
            } catch {
                dataSource.cancel(for: self)
                self.downstreamState = .consumed(.failure(error))
                promise.fail(error)
                return
            }

        case .waitingForAll(var rows, let promise, let dataSource):
            newRows.forEach { data in
                let row = PostgresRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription)
                rows.append(row)
            }
            self.downstreamState = .waitingForAll(rows, promise, dataSource)
            // immediately request more
            dataSource.request(for: self)

        case .asyncSequence(let consumer, let source, _):
            let yieldResult = consumer.yield(contentsOf: newRows)
            self.executeActionBasedOnYieldResult(yieldResult, source: source)
            
        case .consumed(.success):
            preconditionFailure("How can we receive further rows, if we are supposed to be done")
            
        case .consumed(.failure):
            break
        }
    }
    
    internal func receive(completion result: Result<String, any Error>) {
        self.eventLoop.preconditionInEventLoop()
        
        switch result {
        case .success(let commandTag):
            self.receiveEnd(commandTag)
        case .failure(let error):
            self.receiveError(error)
        }
    }
        
    private func receiveEnd(_ commandTag: String) {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming(buffer: let buffer, _)):
            self.downstreamState = .waitingForConsumer(.finished(buffer: buffer, summary: .tag(commandTag)))

        case .waitingForConsumer(.finished), .waitingForConsumer(.failure), .consumed(.success(.emptyResponse)):
            preconditionFailure("How can we get another end, if an end was already signalled?")
            
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(.success(.tag(commandTag)))
            self.finishTracingIfNeeded()
            promise.succeed(())
            
        case .waitingForAll(let rows, let promise, _):
            self.downstreamState = .consumed(.success(.tag(commandTag)))
            self.finishTracingIfNeeded()
            promise.succeed(rows)

        case .asyncSequence(let source, _, let onFinish):
            self.downstreamState = .consumed(.success(.tag(commandTag)))
            self.asyncSequenceCompletedSuccessfully.store(true, ordering: .relaxed)
            source.finish()
            onFinish()

        case .consumed(.success(.tag)), .consumed(.failure):
            break
        }
    }
        
    private func receiveError(_ error: any Error) {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming):
            self.downstreamState = .waitingForConsumer(.failure(error))
            self.failTracingIfNeeded(error)
            
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure), .consumed(.success(.emptyResponse)):
            preconditionFailure("How can we get another end, if an end was already signalled?")
            
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(.failure(error))
            self.failTracingIfNeeded(error)
            promise.fail(error)
            
        case .waitingForAll(_, let promise, _):
            self.downstreamState = .consumed(.failure(error))
            self.failTracingIfNeeded(error)
            promise.fail(error)

        case .asyncSequence(let consumer, _, let onFinish):
            self.downstreamState = .consumed(.failure(error))
            self.failTracingIfNeeded(error)
            consumer.finish(error)
            onFinish()

        case .consumed(.success(.tag)), .consumed(.failure):
            break
        }
    }

    private func executeActionBasedOnYieldResult(_ yieldResult: AsyncSequenceSource.YieldResult, source: any PSQLRowsDataSource) {
        self.eventLoop.preconditionInEventLoop()
        switch yieldResult {
        case .dropped:
            // ignore
            break

        case .produceMore:
            source.request(for: self)

        case .stopProducing:
            // ignore
            break
        }
    }

    private func withTracingContext<T>(_ body: () throws -> T) rethrows -> T {
        guard let tracing = self.tracing else {
            return try body()
        }
        return try tracing.span.withContext {
            try body()
        }
    }

    private func finishTracingIfNeeded() {
        guard let tracing = self.tracing, tracing.managesLifecycle else {
            return
        }
        tracing.span.succeed()
    }

    private func failTracingIfNeeded(_ error: any Error) {
        guard let tracing = self.tracing, tracing.managesLifecycle else {
            return
        }
        tracing.span.fail(error)
    }
    
    var commandTag: String {
        guard case .consumed(.success(let consumed)) = self.downstreamState else {
            preconditionFailure("commandTag may only be called if all rows have been consumed")
        }
        switch consumed {
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
        // End the span synchronously, before the event loop hop, so it closes the moment the
        // consumer's `for try await` loop exits preventing overlap with the next sequential query.
        if let tracing = self.tracing, tracing.managesLifecycle {
            if self.asyncSequenceCompletedSuccessfully.load(ordering: .relaxed) {
                tracing.span.succeed()
            } else {
                tracing.span.fail(CancellationError())
            }
        }

        if self.eventLoop.inEventLoop {
            self.didTerminate0()
        } else {
            self.eventLoop.execute {
                self.didTerminate0()
            }
        }
    }

    private func didTerminate0() {
        switch self.downstreamState {
        case .consumed(.success), .consumed(.failure):
            break

        case .asyncSequence:
            self.cancel0()

        case .waitingForConsumer, .iteratingRows, .waitingForAll:
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
    }
}

protocol PSQLRowsDataSource {
    
    func request(for stream: PSQLRowStream)
    func cancel(for stream: PSQLRowStream)
    
}
