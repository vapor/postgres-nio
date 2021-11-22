import NIOCore
import Logging

final class PSQLRowStream {
    
    enum RowSource {
        case stream(PSQLRowsDataSource)
        case noRows(Result<String, Error>)
    }
    
    let eventLoop: EventLoop
    let logger: Logger
    
    private enum UpstreamState {
        case streaming(buffer: CircularBuffer<DataRow>, dataSource: PSQLRowsDataSource)
        case finished(buffer: CircularBuffer<DataRow>, commandTag: String)
        case failure(Error)
        case consumed(Result<String, Error>)
        case modifying
    }
    
    private enum DownstreamState {
        case iteratingRows(onRow: (PSQLRow) throws -> (), EventLoopPromise<Void>)
        case waitingForAll(EventLoopPromise<[PSQLRow]>)
        case consuming
    }
    
    internal let rowDescription: [RowDescription.Column]
    private let lookupTable: [String: Int]
    private var upstreamState: UpstreamState
    private var downstreamState: DownstreamState
    private let jsonDecoder: PSQLJSONDecoder
    
    init(rowDescription: [RowDescription.Column],
         queryContext: ExtendedQueryContext,
         eventLoop: EventLoop,
         rowSource: RowSource)
    {
        let buffer = CircularBuffer<DataRow>()
        
        self.downstreamState = .consuming
        switch rowSource {
        case .stream(let dataSource):
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
        case .noRows(.success(let commandTag)):
            self.upstreamState = .finished(buffer: .init(), commandTag: commandTag)
        case .noRows(.failure(let error)):
            self.upstreamState = .failure(error)
        }
        
        self.eventLoop = eventLoop
        self.logger = queryContext.logger
        self.jsonDecoder = queryContext.jsonDecoder
        
        self.rowDescription = rowDescription
        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }
        
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
        
        guard case .consuming = self.downstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.upstreamState {
        case .streaming(_, let dataSource):
            dataSource.request(for: self)
            let promise = self.eventLoop.makePromise(of: [PSQLRow].self)
            self.downstreamState = .waitingForAll(promise)
            return promise.futureResult
            
        case .finished(let buffer, let commandTag):
            self.upstreamState = .modifying
            
            let rows = buffer.map {
                PSQLRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
            }
            
            self.downstreamState = .consuming
            self.upstreamState = .consumed(.success(commandTag))
            return self.eventLoop.makeSucceededFuture(rows)
            
        case .consumed:
            preconditionFailure("We already signaled, that the stream has completed, why are we asked again?")
            
        case .modifying:
            preconditionFailure("Invalid state")
            
        case .failure(let error):
            self.upstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
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
        
        switch self.upstreamState {
        case .streaming(var buffer, let dataSource):
            let promise = self.eventLoop.makePromise(of: Void.self)
            do {
                for data in buffer {
                    let row = PSQLRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription,
                        jsonDecoder: self.jsonDecoder
                    )
                    try onRow(row)
                }
                
                buffer.removeAll()
                self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
                self.downstreamState = .iteratingRows(onRow: onRow, promise)
                // immediately request more
                dataSource.request(for: self)
            } catch {
                self.upstreamState = .failure(error)
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
                        columns: self.rowDescription,
                        jsonDecoder: self.jsonDecoder
                    )
                    try onRow(row)
                }
                
                self.upstreamState = .consumed(.success(commandTag))
                self.downstreamState = .consuming
                return self.eventLoop.makeSucceededVoidFuture()
            } catch {
                self.upstreamState = .consumed(.failure(error))
                return self.eventLoop.makeFailedFuture(error)
            }
            
        case .consumed:
            preconditionFailure("We already signaled, that the stream has completed, why are we asked again?")
            
        case .modifying:
            preconditionFailure("Invalid state")
            
        case .failure(let error):
            self.upstreamState = .consumed(.failure(error))
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
        
        guard case .streaming(var buffer, let dataSource) = self.upstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.downstreamState {
        case .iteratingRows(let onRow, let promise):
            precondition(buffer.isEmpty)
            do {
                for data in newRows {
                    let row = PSQLRow(
                        data: data,
                        lookupTable: self.lookupTable,
                        columns: self.rowDescription,
                        jsonDecoder: self.jsonDecoder
                    )
                    try onRow(row)
                }
                // immediately request more
                dataSource.request(for: self)
            } catch {
                dataSource.cancel(for: self)
                self.upstreamState = .failure(error)
                promise.fail(error)
                return
            }
        case .waitingForAll:
            self.upstreamState = .modifying
            buffer.append(contentsOf: newRows)
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
            
            // immediately request more
            dataSource.request(for: self)
            
        case .consuming:
            // this might happen, if the query has finished while the user is consuming data
            // we don't need to ask for more since the user is consuming anyway
            self.upstreamState = .modifying
            buffer.append(contentsOf: newRows)
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
        }
    }
    
    internal func receive(completion result: Result<String, Error>) {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .streaming(let oldBuffer, _) = self.upstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.downstreamState {
        case .iteratingRows(_, let promise):
            precondition(oldBuffer.isEmpty)
            self.downstreamState = .consuming
            self.upstreamState = .consumed(result)
            switch result {
            case .success:
                promise.succeed(())
            case .failure(let error):
                promise.fail(error)
            }
            
            
        case .consuming:
            switch result {
            case .success(let commandTag):
                self.upstreamState = .finished(buffer: oldBuffer, commandTag: commandTag)
            case .failure(let error):
                self.upstreamState = .failure(error)
            }

        case .waitingForAll(let promise):
            switch result {
            case .failure(let error):
                self.upstreamState = .consumed(.failure(error))
                promise.fail(error)
            case .success(let commandTag):
                let rows = oldBuffer.map {
                    PSQLRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
                }
                self.upstreamState = .consumed(.success(commandTag))
                promise.succeed(rows)
            }
        }
    }
    
    func cancel() {
        guard case .streaming(_, let dataSource) = self.upstreamState else {
            // We don't need to cancel any upstream resource. All needed data is already
            // included in this
            return
        }
        
        dataSource.cancel(for: self)
    }
    
    var commandTag: String {
        guard case .consumed(.success(let commandTag)) = self.upstreamState else {
            preconditionFailure("commandTag may only be called if all rows have been consumed")
        }
        return commandTag
    }
}

protocol PSQLRowsDataSource {
    
    func request(for stream: PSQLRowStream)
    func cancel(for stream: PSQLRowStream)
    
}
