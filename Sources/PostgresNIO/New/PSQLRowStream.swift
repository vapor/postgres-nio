import NIOCore
import Logging

final class PSQLRowStream {
    enum RowSource {
        case stream(PSQLRowsDataSource)
        case noRows(Result<String, Error>)
    }
    
    let eventLoop: EventLoop
    let logger: Logger
    
    private enum BufferState {
        case streaming(buffer: CircularBuffer<DataRow>, dataSource: PSQLRowsDataSource)
        case finished(buffer: CircularBuffer<DataRow>, commandTag: String)
        case failure(Error)
    }
    
    private enum DownstreamState {
        case waitingForConsumer(BufferState)
        case iteratingRows(onRow: (PSQLRow) throws -> (), EventLoopPromise<Void>, PSQLRowsDataSource)
        case waitingForAll([PSQLRow], EventLoopPromise<[PSQLRow]>, PSQLRowsDataSource)
        case consumed(Result<String, Error>)
        
        #if swift(>=5.5) && canImport(_Concurrency)
        case asyncSequence(AsyncStreamConsumer, PSQLRowsDataSource)
        #endif
    }
    
    internal let rowDescription: [RowDescription.Column]
    private let lookupTable: [String: Int]
    private var downstreamState: DownstreamState
    
    init(rowDescription: [RowDescription.Column],
         queryContext: ExtendedQueryContext,
         eventLoop: EventLoop,
         rowSource: RowSource)
    {
        let bufferState: BufferState
        switch rowSource {
        case .stream(let dataSource):
            bufferState = .streaming(buffer: .init(), dataSource: dataSource)
        case .noRows(.success(let commandTag)):
            bufferState = .finished(buffer: .init(), commandTag: commandTag)
        case .noRows(.failure(let error)):
            bufferState = .failure(error)
        }
        
        self.downstreamState = .waitingForConsumer(bufferState)
        
        self.eventLoop = eventLoop
        self.logger = queryContext.logger
        
        self.rowDescription = rowDescription

        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }
    
    // MARK: Async Sequence
    
    #if swift(>=5.5) && canImport(_Concurrency)
    func asyncSequence() -> PostgresRowSequence {
        self.eventLoop.preconditionInEventLoop()

        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        let consumer = AsyncStreamConsumer(
            lookupTable: self.lookupTable,
            columns: self.rowDescription
        )
        
        switch bufferState {
        case .streaming(let bufferedRows, let dataSource):
            consumer.startStreaming(bufferedRows, upstream: self)
            self.downstreamState = .asyncSequence(consumer, dataSource)
            
        case .finished(let buffer, let commandTag):
            consumer.startCompleted(buffer, commandTag: commandTag)
            self.downstreamState = .consumed(.success(commandTag))
            
        case .failure(let error):
            consumer.startFailed(error)
            self.downstreamState = .consumed(.failure(error))
        }
        
        return PostgresRowSequence(consumer)
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
            
        case .asyncSequence(_, let dataSource):
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
        case .asyncSequence(let consumer, let dataSource):
            let error = PSQLError.connectionClosed
            self.downstreamState = .consumed(.failure(error))
            consumer.receive(completion: .failure(error))
            dataSource.cancel(for: self)

        case .consumed:
            return

        case .waitingForConsumer, .iteratingRows, .waitingForAll:
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
    }
    #endif
    
    // MARK: Consume in array
        
    func all() -> EventLoopFuture<[PSQLRow]> {
        if self.eventLoop.inEventLoop {
            return self.all0()
        } else {
            return self.eventLoop.flatSubmit {
                self.all0()
            }
        }
    }
    
    private func all0() -> EventLoopFuture<[PSQLRow]> {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        switch bufferState {
        case .streaming(let bufferedRows, let dataSource):
            let promise = self.eventLoop.makePromise(of: [PSQLRow].self)
            let rows = bufferedRows.map { data in
                PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription)
            }
            self.downstreamState = .waitingForAll(rows, promise, dataSource)
            // immediately request more
            dataSource.request(for: self)
            return promise.futureResult
            
        case .finished(let buffer, let commandTag):
            let rows = buffer.map {
                PSQLRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription)
            }
            
            self.downstreamState = .consumed(.success(commandTag))
            return self.eventLoop.makeSucceededFuture(rows)
            
        case .failure(let error):
            self.downstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    // MARK: Consume on EventLoop
    
    func onRow(_ onRow: @escaping (PSQLRow) throws -> ()) -> EventLoopFuture<Void> {
        if self.eventLoop.inEventLoop {
            return self.onRow0(onRow)
        } else {
            return self.eventLoop.flatSubmit {
                self.onRow0(onRow)
            }
        }
    }
    
    private func onRow0(_ onRow: @escaping (PSQLRow) throws -> ()) -> EventLoopFuture<Void> {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .waitingForConsumer(let bufferState) = self.downstreamState else {
            preconditionFailure("Invalid state: \(self.downstreamState)")
        }
        
        switch bufferState {
        case .streaming(var buffer, let dataSource):
            let promise = self.eventLoop.makePromise(of: Void.self)
            do {
                for data in buffer {
                    let row = PSQLRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try onRow(row)
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
            
        case .finished(let buffer, let commandTag):
            do {
                for data in buffer {
                    let row = PSQLRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try onRow(row)
                }
                
                self.downstreamState = .consumed(.success(commandTag))
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
    
    internal func noticeReceived(_ notice: PSQLBackendMessage.NoticeResponse) {
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
                    let row = PSQLRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription
                    )
                    try onRow(row)
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
                let row = PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription)
                rows.append(row)
            }
            self.downstreamState = .waitingForAll(rows, promise, dataSource)
            // immediately request more
            dataSource.request(for: self)
        
        #if swift(>=5.5) && canImport(_Concurrency)
        case .asyncSequence(let consumer, _):
            consumer.receive(newRows)
        #endif
            
        case .consumed(.success):
            preconditionFailure("How can we receive further rows, if we are supposed to be done")
            
        case .consumed(.failure):
            break
        }
    }
    
    internal func receive(completion result: Result<String, Error>) {
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
            self.downstreamState = .waitingForConsumer(.finished(buffer: buffer, commandTag: commandTag))
            
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("How can we get another end, if an end was already signalled?")
            
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(.success(commandTag))
            promise.succeed(())
            
        case .waitingForAll(let rows, let promise, _):
            self.downstreamState = .consumed(.success(commandTag))
            promise.succeed(rows)
            
        #if swift(>=5.5) && canImport(_Concurrency)
        case .asyncSequence(let consumer, _):
            consumer.receive(completion: .success(commandTag))
            self.downstreamState = .consumed(.success(commandTag))
        #endif
            
        case .consumed:
            break
        }
    }
        
    private func receiveError(_ error: Error) {
        switch self.downstreamState {
        case .waitingForConsumer(.streaming):
            self.downstreamState = .waitingForConsumer(.failure(error))
            
        case .waitingForConsumer(.finished), .waitingForConsumer(.failure):
            preconditionFailure("How can we get another end, if an end was already signalled?")
            
        case .iteratingRows(_, let promise, _):
            self.downstreamState = .consumed(.failure(error))
            promise.fail(error)
            
        case .waitingForAll(_, let promise, _):
            self.downstreamState = .consumed(.failure(error))
            promise.fail(error)
            
        #if swift(>=5.5) && canImport(_Concurrency)
        case .asyncSequence(let consumer, _):
            consumer.receive(completion: .failure(error))
            self.downstreamState = .consumed(.failure(error))
        #endif
            
        case .consumed:
            break
        }
    }
    
    var commandTag: String {
        guard case .consumed(.success(let commandTag)) = self.downstreamState else {
            preconditionFailure("commandTag may only be called if all rows have been consumed")
        }
        return commandTag
    }
}

protocol PSQLRowsDataSource {
    
    func request(for stream: PSQLRowStream)
    func cancel(for stream: PSQLRowStream)
    
}
