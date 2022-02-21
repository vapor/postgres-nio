/// NOTE: THIS FILE IS AUTO-GENERATED BY dev/generate-psqlrow-multi-decode.sh

#if swift(>=5.5) && canImport(_Concurrency)
extension PSQLRowSequence {
    @inlinable
    public func decode<T0: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0)> {
        self.map { row in
            try row.decode(T0.self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1)> {
        self.map { row in
            try row.decode((T0, T1).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2)> {
        self.map { row in
            try row.decode((T0, T1, T2).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, T9: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8, T9).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, T9: PostgresDecodable, T10: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, T9: PostgresDecodable, T10: PostgresDecodable, T11: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, T9: PostgresDecodable, T10: PostgresDecodable, T11: PostgresDecodable, T12: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, T9: PostgresDecodable, T10: PostgresDecodable, T11: PostgresDecodable, T12: PostgresDecodable, T13: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13).self, context: context, file: file, line: line)
        }
    }

    @inlinable
    public func decode<T0: PostgresDecodable, T1: PostgresDecodable, T2: PostgresDecodable, T3: PostgresDecodable, T4: PostgresDecodable, T5: PostgresDecodable, T6: PostgresDecodable, T7: PostgresDecodable, T8: PostgresDecodable, T9: PostgresDecodable, T10: PostgresDecodable, T11: PostgresDecodable, T12: PostgresDecodable, T13: PostgresDecodable, T14: PostgresDecodable, JSONDecoder: PostgresJSONDecoder>(_: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14).Type, context: PostgresDecodingContext<JSONDecoder>, file: String = #file, line: Int = #line) -> AsyncThrowingMapSequence<PSQLRowSequence, (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)> {
        self.map { row in
            try row.decode((T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14).self, context: context, file: file, line: line)
        }
    }
}
#endif
