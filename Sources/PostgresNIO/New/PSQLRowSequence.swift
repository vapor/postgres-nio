import NIOCore
import NIOConcurrencyHelpers

#if swift(>=5.5) && canImport(_Concurrency)

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
public struct PSQLRowSequence: AsyncSequence {
    public typealias Element = PSQLRow
    public typealias AsyncIterator = Iterator
    
    final class _Internal {
        
        let consumer: AsyncStreamConsumer
        
        init(consumer: AsyncStreamConsumer) {
            self.consumer = consumer
        }
        
        deinit {
            // if no iterator was created, we need to cancel the stream
            self.consumer.sequenceDeinitialized()
        }
        
        func makeAsyncIterator() -> Iterator {
            self.consumer.makeAsyncIterator()
        }
    }
    
    let _internal: _Internal
    
    init(_ consumer: AsyncStreamConsumer) {
        self._internal = .init(consumer: consumer)
    }
    
    public func makeAsyncIterator() -> Iterator {
        self._internal.makeAsyncIterator()
    }
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension PSQLRowSequence {
    public struct Iterator: AsyncIteratorProtocol {
        public typealias Element = PSQLRow
        
        let _internal: _Internal
        
        init(consumer: AsyncStreamConsumer) {
            self._internal = _Internal(consumer: consumer)
        }
        
        public mutating func next() async throws -> PSQLRow? {
            try await self._internal.next()
        }
        
        final class _Internal {
            struct ID: Hashable {
                let objectID: ObjectIdentifier
                
                init(_ object: _Internal) {
                    self.objectID = ObjectIdentifier(object)
                }
            }
            
            var id: ID { ID(self) }
            
            let consumer: AsyncStreamConsumer
            
            init(consumer: AsyncStreamConsumer) {
                self.consumer = consumer
            }
            
            func next() async throws -> PSQLRow? {
                try await self.consumer.next(for: self.id)
            }
        }
    }
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
final class AsyncStreamConsumer {
    let lock = Lock()
    
    let lookupTable: [String: Int]
    let columns: [RowDescription.Column]
    let jsonDecoder: PSQLJSONDecoder
    private var state: StateMachine
    
    init(
        lookupTable: [String: Int],
        columns: [RowDescription.Column],
        jsonDecoder: PSQLJSONDecoder
    ) {
        self.state = StateMachine()
        
        self.lookupTable = lookupTable
        self.columns = columns
        self.jsonDecoder = jsonDecoder
    }
    
    func startCompleted(_ buffer: CircularBuffer<DataRow>, commandTag: String) {
        self.lock.withLock {
            self.state.finished(buffer, commandTag: commandTag)
        }
    }
    
    func startStreaming(_ buffer: CircularBuffer<DataRow>, upstream: PSQLRowStream) {
        self.lock.withLock {
            self.state.buffered(buffer, upstream: upstream)
        }
    }
    
    func startFailed(_ error: Error) {
        self.lock.withLock {
            self.state.failed(error)
        }
    }
    
    func receive(_ newRows: [DataRow]) {
        let receiveAction = self.lock.withLock {
            self.state.receive(newRows)
        }
        
        switch receiveAction {
        case .succeed(let continuation, let data, signalDemandTo: let source):
            let row = PSQLRow(
                data: data,
                lookupTable: self.lookupTable,
                columns: self.columns,
                jsonDecoder: self.jsonDecoder
            )
            continuation.resume(returning: row)
            source?.demand()
            
        case .none:
            break
        }
    }
    
    func receive(completion result: Result<String, Error>) {
        let completionAction = self.lock.withLock {
            self.state.receive(completion: result)
        }
        
        switch completionAction {
        case .succeed(let continuation):
            continuation.resume(returning: nil)
            
        case .fail(let continuation, let error):
            continuation.resume(throwing: error)
            
        case .none:
            break
        }
    }
    
    func sequenceDeinitialized() {
        let action = self.lock.withLock {
            self.state.sequenceDeinitialized()
        }
        
        switch action {
        case .cancelStream(let source):
            source.cancel()
        case .none:
            break
        }
    }
    
    func makeAsyncIterator() -> PSQLRowSequence.Iterator {
        let iterator = PSQLRowSequence.Iterator(consumer: self)
        self.lock.withLock {
            self.state.registerAsyncIteratorID(ObjectIdentifier(iterator._internal))
        }
        return iterator
    }
    
    func next(for id: PSQLRowSequence.Iterator._Internal.ID) async throws -> PSQLRow? {
        self.lock.lock()
        switch self.state.next() {
        case .returnNil:
            self.lock.unlock()
            return nil
            
        case .returnRow(let data, signalDemandTo: let source):
            self.lock.unlock()
            source?.demand()
            return PSQLRow(
                data: data,
                lookupTable: self.lookupTable,
                columns: self.columns,
                jsonDecoder: self.jsonDecoder
            )
            
        case .throwError(let error):
            self.lock.unlock()
            throw error
            
        case .hitSlowPath:
            return try await withCheckedThrowingContinuation { continuation in
                let slowPathAction = self.state.next(for: continuation)
                self.lock.unlock()
                switch slowPathAction {
                case .signalDemand(let source):
                    source.demand()
                case .none:
                    break
                }
            }
        }
    }
    
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension AsyncStreamConsumer {
    struct StateMachine {
        enum UpstreamState {
            enum DemandState {
                case canAskForMore
                case waitingForMore(CheckedContinuation<PSQLRow?, Error>?)
            }
            
            case initialized
            case streaming(AdaptiveRowBuffer, PSQLRowStream, DemandState)
            case finished(AdaptiveRowBuffer, String)
            case failed(Error)
            case done
            
            case modifying
        }
        
        enum DownstreamState {
            case sequenceCreated
            case iteratorCreated(ObjectIdentifier)
        }
        
        var upstreamState: UpstreamState
        var downstreamState: DownstreamState
        
        init() {
            self.upstreamState = .initialized
            self.downstreamState = .sequenceCreated
        }
        
        mutating func buffered(_ buffer: CircularBuffer<DataRow>, upstream: PSQLRowStream) {
            guard case .initialized = self.upstreamState else {
                preconditionFailure("Invalid upstream state: \(self.upstreamState)")
            }
            let adaptive = AdaptiveRowBuffer(buffer)
            self.upstreamState = .streaming(adaptive, upstream, buffer.isEmpty ? .waitingForMore(nil) : .canAskForMore)
        }
        
        mutating func finished(_ buffer: CircularBuffer<DataRow>, commandTag: String) {
            guard case .initialized = self.upstreamState else {
                preconditionFailure("Invalid upstream state: \(self.upstreamState)")
            }
            let adaptive = AdaptiveRowBuffer(buffer)
            self.upstreamState = .finished(adaptive, commandTag)
        }
        
        mutating func failed(_ error: Error) {
            guard case .initialized = self.upstreamState else {
                preconditionFailure("Invalid upstream state: \(self.upstreamState)")
            }
            self.upstreamState = .failed(error)
        }
        
        mutating func registerAsyncIteratorID(_ id: ObjectIdentifier) {
            switch self.downstreamState {
            case .sequenceCreated:
                self.downstreamState = .iteratorCreated(id)
            case .iteratorCreated:
                preconditionFailure("An iterator already exists")
            }
        }
        
        enum SequenceDeinitializedAction {
            case cancelStream(PSQLRowStream)
            case none
        }
        
        mutating func sequenceDeinitialized() -> SequenceDeinitializedAction {
            switch (self.downstreamState, self.upstreamState) {
            case (.sequenceCreated, .initialized):
                preconditionFailure()
                
            case (.sequenceCreated, .streaming(_, let source, _)):
                return .cancelStream(source)
                
            case (.sequenceCreated, .finished),
                 (.sequenceCreated, .done),
                 (.sequenceCreated, .failed):
                return .none
                
            case (.iteratorCreated, _):
                return .none
                
            case (_, .modifying):
                preconditionFailure()
            }
        }
        
        enum NextFastPathAction {
            case hitSlowPath
            case throwError(Error)
            case returnRow(DataRow, signalDemandTo: PSQLRowStream?)
            case returnNil
        }
        
        mutating func next() -> NextFastPathAction {
            switch self.upstreamState {
            case .initialized:
                preconditionFailure()

            case .streaming(var buffer, let source, .canAskForMore):
                self.upstreamState = .modifying
                guard let (data, demand) = buffer.popFirst() else {
                    self.upstreamState = .streaming(buffer, source, .canAskForMore)
                    return .hitSlowPath
                }
                if demand {
                    self.upstreamState = .streaming(buffer, source, .waitingForMore(.none))
                    return .returnRow(data, signalDemandTo: source)
                }
                self.upstreamState = .streaming(buffer, source, .canAskForMore)
                return .returnRow(data, signalDemandTo: nil)

            case .streaming(var buffer, let source, .waitingForMore(.none)):
                self.upstreamState = .modifying
                guard let (data, _) = buffer.popFirst() else {
                    self.upstreamState = .streaming(buffer, source, .waitingForMore(.none))
                    return .hitSlowPath
                }
                
                self.upstreamState = .streaming(buffer, source, .waitingForMore(.none))
                return .returnRow(data, signalDemandTo: nil)

            case .streaming(_, _, .waitingForMore(.some)):
                preconditionFailure()

            case .finished(var buffer, let commandTag):
                self.upstreamState = .modifying
                guard let (data, _) = buffer.popFirst() else {
                    self.upstreamState = .done
                    return .returnNil
                }
                
                self.upstreamState = .finished(buffer, commandTag)
                return .returnRow(data, signalDemandTo: nil)

            case .failed(let error):
                self.upstreamState = .done
                return .throwError(error)

            case .done:
                return .returnNil

            case .modifying:
                preconditionFailure()
            }
        }

        enum NextSlowPathAction {
            case signalDemand(PSQLRowStream)
            case none
        }
        
        mutating func next(for continuation: CheckedContinuation<PSQLRow?, Error>) -> NextSlowPathAction {
            switch self.upstreamState {
            case .initialized:
                preconditionFailure()
                
            case .streaming(let buffer, let source, .canAskForMore):
                precondition(buffer.isEmpty)
                self.upstreamState = .streaming(buffer, source, .waitingForMore(continuation))
                return .signalDemand(source)
                
            case .streaming(let buffer, let source, .waitingForMore(.none)):
                precondition(buffer.isEmpty)
                self.upstreamState = .streaming(buffer, source, .waitingForMore(continuation))
                return .none
            
            case .streaming(_, _, .waitingForMore(.some)):
                preconditionFailure()
                
            case .finished:
                preconditionFailure()
                
            case .failed:
                preconditionFailure()
                
            case .done:
                preconditionFailure()
                
            case .modifying:
                preconditionFailure()
            }
        }
        
        enum ReceiveAction {
            case succeed(CheckedContinuation<PSQLRow?, Error>, DataRow, signalDemandTo: PSQLRowStream?)
            case none
        }
        
        mutating func receive(_ newRows: [DataRow]) -> ReceiveAction {
            precondition(!newRows.isEmpty)
            
            switch self.upstreamState {
            case .streaming(var buffer, let source, .waitingForMore(.some(let continuation))):
                buffer.append(contentsOf: newRows)
                let (first, demand) = buffer.removeFirst()
                if demand {
                    self.upstreamState = .streaming(buffer, source, .waitingForMore(.none))
                    return .succeed(continuation, first, signalDemandTo: source)
                }
                self.upstreamState = .streaming(buffer, source, .canAskForMore)
                return .succeed(continuation, first, signalDemandTo: nil)
            
            case .streaming(var buffer, let source, .waitingForMore(.none)):
                buffer.append(contentsOf: newRows)
                self.upstreamState = .streaming(buffer, source, .canAskForMore)
                return .none
                
            case .streaming(var buffer, let source, .canAskForMore):
                buffer.append(contentsOf: newRows)
                self.upstreamState = .streaming(buffer, source, .canAskForMore)
                return .none
                
            case .initialized, .finished, .done:
                preconditionFailure()
                
            case .failed:
                return .none
                
            case .modifying:
                preconditionFailure()
            }
        }
        
        enum CompletionResult {
            case succeed(CheckedContinuation<PSQLRow?, Error>)
            case fail(CheckedContinuation<PSQLRow?, Error>, Error)
            case none
        }
        
        mutating func receive(completion result: Result<String, Error>) -> CompletionResult {
            switch result {
            case .success(let commandTag):
                return self.receiveEnd(commandTag: commandTag)
            case .failure(let error):
                return self.receiveError(error)
            }
        }
        
        mutating func receiveEnd(commandTag: String) -> CompletionResult {
            switch self.upstreamState {
            case .streaming(let buffer, _, .waitingForMore(.some(let continuation))):
                precondition(buffer.isEmpty)
                self.upstreamState = .done
                return .succeed(continuation)
            
            case .streaming(let buffer, _, .waitingForMore(.none)):
                self.upstreamState = .finished(buffer, commandTag)
                return .none
                
            case .streaming(let buffer, _, .canAskForMore):
                self.upstreamState = .finished(buffer, commandTag)
                return .none
                
            case .initialized, .finished, .done:
                preconditionFailure()
                
            case .failed:
                return .none
                
            case .modifying:
                preconditionFailure()
            }
        }
        
        mutating func receiveError(_ error: Error) -> CompletionResult {
            switch self.upstreamState {
            case .streaming(let buffer, _, .waitingForMore(.some(let continuation))):
                precondition(buffer.isEmpty)
                self.upstreamState = .done
                return .fail(continuation, error)
            
            case .streaming(let buffer, _, .waitingForMore(.none)):
                precondition(buffer.isEmpty)
                self.upstreamState = .failed(error)
                return .none
                
            case .streaming(_, _, .canAskForMore):
                self.upstreamState = .failed(error)
                return .none
                
            case .initialized, .finished, .done:
                preconditionFailure()
                
            case .failed:
                return .none
                
            case .modifying:
                preconditionFailure()
            }
        }
    }
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension PSQLRowSequence {
    func collect() async throws -> [PSQLRow] {
        var result = [PSQLRow]()
        for try await row in self {
            result.append(row)
        }
        return result
    }
}

struct AdaptiveRowBuffer {
    public let minimum: Int
    public let maximum: Int

    private var circularBuffer: CircularBuffer<DataRow>
    private var target: Int
    private var canShrink: Bool = false
    
    private var hasDemand: Bool {
        self.circularBuffer.count < self.maximum
    }
    
    var isEmpty: Bool {
        self.circularBuffer.isEmpty
    }
    
    init() {
        self.minimum = 1
        self.maximum = 16384
        self.target = 256
        self.circularBuffer = CircularBuffer()
    }
    
    init(_ circularBuffer: CircularBuffer<DataRow>) {
        self.minimum = 1
        self.maximum = 16384
        self.target = 64
        self.circularBuffer = circularBuffer
    }
    
    mutating func append<Rows: Sequence>(contentsOf newRows: Rows) where Rows.Element == DataRow {
        self.circularBuffer.append(contentsOf: newRows)
//        print("buffer size: \(self.circularBuffer.count)")
        if self.circularBuffer.count >= self.target, self.canShrink, self.target > self.minimum {
            self.target &>>= 1
//            print("shrink: \(self.target)")
        }
        self.canShrink = true
    }
    
    mutating func removeFirst() -> (DataRow, Bool) {
        let element = self.circularBuffer.removeFirst()
        
        // If the buffer is drained now, we should double our target size.
        if self.circularBuffer.count == 0, self.target < self.maximum {
            self.target = self.target * 2
            self.canShrink = false
//            print("grow: \(self.target)")
        }
        
        return (element, self.circularBuffer.count < self.target)
    }
    
    mutating func popFirst() -> (DataRow, Bool)? {
        guard !self.circularBuffer.isEmpty else {
            return nil
        }
        return self.removeFirst()
    }
}

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
public struct PSQLDecodedRowSequence<T0: PSQLDecodable, T1: PSQLDecodable, T2: PSQLDecodable>: AsyncSequence {
    public typealias AsyncIterator = Iterator
    public typealias Element = (T0, T1, T2)
    
    @usableFromInline
    let upstream: PSQLRowSequence
    @usableFromInline
    let file: String
    @usableFromInline
    let line: Int
    
    @inlinable
    init(_ upstream: PSQLRowSequence, _ t0: T0.Type, _ t1: T1.Type, _ t2: T2.Type, file: String, line: Int) {
        self.upstream = upstream
        self.file = file
        self.line = line
    }
    
    @inlinable
    public func makeAsyncIterator() -> Iterator {
        Iterator(self.upstream.makeAsyncIterator(), file: self.file, line: self.line)
    }
    
    public struct Iterator: AsyncIteratorProtocol {
        public typealias Element = (T0, T1, T2)
        
        @usableFromInline
        var upstream: PSQLRowSequence.Iterator
        
        @usableFromInline
        let file: String
        
        @usableFromInline
        let line: Int
        
        @inlinable
        init(_ upstream: PSQLRowSequence.Iterator, file: String, line: Int) {
            self.upstream = upstream
            self.file = file
            self.line = line
        }
        
        @inlinable
        public mutating func next() async throws -> Element? {
            try await self.upstream.next()?.decode(T0.self, T1.self, T2.self, file: self.file, line: self.line)
        }
    }
}

#endif

struct AbstractStreamConsumer {
    var consumer: Any?
    
    init() {
        self.consumer = nil
    }
    
    #if swift(>=5.5) && canImport(_Concurrency)
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    init(_ consumer: AsyncStreamConsumer) {
        self.consumer = consumer
    }
    #endif
    
    func receive(_ newRows: [DataRow]) {
        #if swift(>=5.5) && canImport(_Concurrency)
        if #available(macOS 12, iOS 15, tvOS 15, watchOS 8, *) {
            (self.consumer as! AsyncStreamConsumer).receive(newRows)
        }
        #endif
    }
    
    func receive(completion result: Result<String, Error>) {
        #if swift(>=5.5) && canImport(_Concurrency)
        if #available(macOS 12, iOS 15, tvOS 15, watchOS 8, *) {
            (self.consumer as! AsyncStreamConsumer).receive(completion: result)
        }
        #endif
    }
}
