#if swift(>=5.5)

@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension PSQLRowSequence {
    
    @inlinable
    public func decode<T0>(_ t0: T0.Type, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, T0>
        where T0: PSQLDecodable
    {
        self.map { try $0.decode(t0, file: file, line: line) }
    }
    
    @inlinable
    public func decode<T0, T1>(_ t0: T0.Type, _ t1: T1.Type, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1)>
        where T0: PSQLDecodable, T1: PSQLDecodable
    {
        self.map { try $0.decode(t0, t1, file: file, line: line) }
    }
    
    @inlinable
    public func decode<T0, T1, T2>(_ t0: T0.Type, _ t1: T1.Type, _ t2: T2.Type, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2)>
        where T0: PSQLDecodable, T1: PSQLDecodable, T2: PSQLDecodable
    {
        self.map { try $0.decode(t0, t1, t2, file: file, line: line) }
    }
    
    @inlinable
    public func decode<T0, T1, T2, T3>(
        _ t0: T0.Type,
        _ t1: T1.Type,
        _ t2: T2.Type,
        _ t3: T3.Type,
        file: String = #file, line: Int = #line
    ) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3)>
        where T0: PSQLDecodable,
              T1: PSQLDecodable,
              T2: PSQLDecodable,
              T3: PSQLDecodable
    {
        self.map { try $0.decode(t0, t1, t2, t3, file: file, line: line) }
    }
    
    @inlinable
    public func decode<T0, T1, T2, T3, T4>(
        _ t0: T0.Type,
        _ t1: T1.Type,
        _ t2: T2.Type,
        _ t3: T3.Type,
        _ t4: T4.Type,
        file: String = #file, line: Int = #line
    ) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4)>
        where T0: PSQLDecodable,
              T1: PSQLDecodable,
              T2: PSQLDecodable,
              T3: PSQLDecodable,
              T4: PSQLDecodable
    {
        self.map { try $0.decode(t0, t1, t2, t3, t4, file: file, line: line) }
    }
}

#endif
