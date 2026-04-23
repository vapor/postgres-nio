import Atomics
import Foundation
import NIOCore
import Testing
@testable import PostgresNIO

@Suite struct PostgresJSONB_PSQLCodableTests {

    // MARK: - Fixtures

    /// A simple model that is Codable but NOT PostgresCodable.
    /// This is the exact use case the wrappers exist to serve.
    struct SampleModel: Codable, Equatable, Sendable {
        var name: String
        var count: Int
    }

    struct Nested: Codable, Equatable, Sendable {
        struct Inner: Codable, Equatable, Sendable {
            var value: Double
        }
        var label: String
        var inner: Inner
    }

    struct Empty: Codable, Equatable, Sendable {}

    // MARK: - PostgresJSONB: type metadata

    @Test func jsonbStaticType() {
        #expect(PostgresJSONB<SampleModel>.psqlType == .jsonb)
        #expect(PostgresJSONB<SampleModel>.psqlFormat == .binary)
    }

    @Test func jsonbInstanceTypeMatchesStatic() {
        let wrapper = PostgresJSONB(SampleModel(name: "test", count: 1))
        #expect(wrapper.psqlType == PostgresJSONB<SampleModel>.psqlType)
        #expect(wrapper.psqlFormat == PostgresJSONB<SampleModel>.psqlFormat)
    }

    // MARK: - PostgresJSONB: encode

    @Test func jsonbEncodesVersionByteAndValidJSON() throws {
        let model = SampleModel(name: "hello", count: 42)
        let wrapper = PostgresJSONB(model)

        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)

        // first byte: JSONB version byte 0x01
        #expect(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self) == 0x01)
        buffer.moveReaderIndex(forwardBy: 1)

        // remaining bytes: valid JSON that round-trips
        let decoded = try JSONDecoder().decode(SampleModel.self, from: Data(buffer.readableBytesView))
        #expect(decoded == model)
    }

    @Test func jsonbEncodesNestedStruct() throws {
        let model = Nested(label: "outer", inner: .init(value: 3.14))
        var buffer = ByteBuffer()
        try PostgresJSONB(model).encode(into: &buffer, context: .default)

        #expect(buffer.readInteger(as: UInt8.self) == 0x01)
        let decoded = try JSONDecoder().decode(Nested.self, from: Data(buffer.readableBytesView))
        #expect(decoded == model)
    }

    @Test func jsonbEncodesEmptyStruct() throws {
        var buffer = ByteBuffer()
        try PostgresJSONB(Empty()).encode(into: &buffer, context: .default)

        #expect(buffer.readInteger(as: UInt8.self) == 0x01)
        #expect(throws: Never.self) {
            try JSONDecoder().decode(Empty.self, from: Data(buffer.readableBytesView))
        }
    }

    @Test func jsonbEncodesPrimitiveString() throws {
        var buffer = ByteBuffer()
        try PostgresJSONB("just a string").encode(into: &buffer, context: .default)

        #expect(buffer.readInteger(as: UInt8.self) == 0x01)
        let decoded = try JSONDecoder().decode(String.self, from: Data(buffer.readableBytesView))
        #expect(decoded == "just a string")
    }

    @Test func jsonbEncodesDictionary() throws {
        let dict: [String: Int] = ["a": 1, "b": 2]
        var buffer = ByteBuffer()
        try PostgresJSONB(dict).encode(into: &buffer, context: .default)

        #expect(buffer.readInteger(as: UInt8.self) == 0x01)
        let decoded = try JSONDecoder().decode([String: Int].self, from: Data(buffer.readableBytesView))
        #expect(decoded == dict)
    }

    /// Wrapping an array in PostgresJSONB should produce a single JSONB value
    /// containing a JSON array — NOT a Postgres JSONB[] array type.
    @Test func jsonbWrappingArrayProducesSingleJSONBNotArray() throws {
        let arr = [1, 2, 3]
        let wrapper = PostgresJSONB(arr)
        #expect(wrapper.psqlType == .jsonb)

        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)
        #expect(buffer.readInteger(as: UInt8.self) == 0x01)
        let decoded = try JSONDecoder().decode([Int].self, from: Data(buffer.readableBytesView))
        #expect(decoded == arr)
    }

    // MARK: - PostgresJSONB: through PostgresBindings

    @Test func jsonbAppendToBindings() throws {
        let model = SampleModel(name: "bound", count: 7)
        var bindings = PostgresBindings()
        try bindings.append(PostgresJSONB(model), context: .default)

        #expect(bindings.count == 1)
        #expect(bindings.metadata[0].dataType == .jsonb)
        #expect(bindings.metadata[0].format == .binary)

        // encodeRaw writes: [Int32 length][payload]
        var bytes = bindings.bytes
        let length = bytes.readInteger(as: Int32.self)!
        #expect(length > 0)
        #expect(bytes.readInteger(as: UInt8.self) == 0x01)
        let jsonSlice = bytes.readSlice(length: Int(length) - 1)!
        let decoded = try JSONDecoder().decode(SampleModel.self, from: Data(jsonSlice.readableBytesView))
        #expect(decoded == model)
    }

    /// Verify PostgresBindings byte layout matches what PostgresQueryTests expects:
    /// the exact bytes produced by encodeRaw for a known model.
    @Test func jsonbBindingsExactBytes() throws {
        let model = SampleModel(name: "x", count: 0)
        var bindings = PostgresBindings()
        try bindings.append(PostgresJSONB(model), context: .default)

        // Build the expected buffer manually
        let jsonPayload = try JSONEncoder().encode(model)
        var expected = ByteBuffer()
        expected.writeInteger(Int32(1 + jsonPayload.count)) // version byte + json
        expected.writeInteger(UInt8(0x01))
        expected.writeData(jsonPayload)

        #expect(bindings.bytes == expected)
    }

    // MARK: - PostgresJSONBArray: type metadata

    @Test func jsonbArrayStaticType() {
        #expect(PostgresJSONBArray<SampleModel>.psqlType == .jsonbArray)
        #expect(PostgresJSONBArray<SampleModel>.psqlFormat == .binary)
    }

    @Test func jsonbArrayInstanceTypeMatchesStatic() {
        let wrapper = PostgresJSONBArray([SampleModel(name: "a", count: 1)])
        #expect(wrapper.psqlType == PostgresJSONBArray<SampleModel>.psqlType)
        #expect(wrapper.psqlFormat == PostgresJSONBArray<SampleModel>.psqlFormat)
    }

    // MARK: - PostgresJSONBArray: empty

    @Test func jsonbArrayEncodesEmpty() throws {
        let wrapper = PostgresJSONBArray<SampleModel>([])
        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)

        // Header: dimensions=0, has-null=0, element-oid=jsonb
        #expect(buffer.readInteger(as: UInt32.self) == 0) // dimensions
        #expect(buffer.readInteger(as: Int32.self) == 0)   // has-null
        #expect(buffer.readInteger(as: UInt32.self) == PostgresDataType.jsonb.rawValue)
        #expect(buffer.readableBytes == 0) // nothing after header
    }

    // MARK: - PostgresJSONBArray: wire format

    /// Parse the full Postgres binary array wire format byte-by-byte, matching
    /// the structure tested in Array+PSQLCodableTests for native arrays.
    @Test func jsonbArrayWireFormatTwoElements() throws {
        let models = [
            SampleModel(name: "first", count: 1),
            SampleModel(name: "second", count: 2),
        ]
        let wrapper = PostgresJSONBArray(models)

        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)

        // --- Array header ---
        #expect(buffer.readInteger(as: UInt32.self) == 1) // dimensions
        #expect(buffer.readInteger(as: Int32.self) == 0)   // has-null flag
        #expect(buffer.readInteger(as: UInt32.self) == PostgresDataType.jsonb.rawValue) // element OID

        // --- Dimension descriptor ---
        #expect(buffer.readInteger(as: Int32.self) == 2)   // array length
        #expect(buffer.readInteger(as: Int32.self) == 1)   // lower bound (1-based)

        // --- Elements ---
        for (index, expected) in models.enumerated() {
            let elementLength = try #require(buffer.readInteger(as: Int32.self))
            #expect(elementLength > 1, "Element \(index) must be at least version byte + JSON")

            var elementSlice = try #require(buffer.readSlice(length: Int(elementLength)))
            #expect(elementSlice.readInteger(as: UInt8.self) == 0x01, "JSONB version byte at index \(index)")

            let decoded = try JSONDecoder().decode(SampleModel.self, from: Data(elementSlice.readableBytesView))
            #expect(decoded == expected, "Decoded element at index \(index) must match")
        }

        #expect(buffer.readableBytes == 0, "Buffer must be fully consumed")
    }

    @Test func jsonbArraySingleElement() throws {
        let wrapper = PostgresJSONBArray([SampleModel(name: "solo", count: 99)])
        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)

        // Skip header: dims(4) + hasNull(4) + oid(4) + length(4) + lowerBound(4) = 20
        buffer.moveReaderIndex(forwardBy: 20)

        let elementLength = try #require(buffer.readInteger(as: Int32.self))
        #expect(buffer.readInteger(as: UInt8.self) == 0x01)

        let jsonSlice = try #require(buffer.readSlice(length: Int(elementLength) - 1))
        let decoded = try JSONDecoder().decode(SampleModel.self, from: Data(jsonSlice.readableBytesView))
        #expect(decoded.name == "solo")
        #expect(decoded.count == 99)
    }

    @Test func jsonbArrayManyElements() throws {
        let models = (0..<100).map { SampleModel(name: "item-\($0)", count: $0) }
        let wrapper = PostgresJSONBArray(models)

        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)

        // Verify header
        #expect(buffer.readInteger(as: UInt32.self) == 1)
        buffer.moveReaderIndex(forwardBy: 4) // has-null
        buffer.moveReaderIndex(forwardBy: 4) // element OID
        #expect(buffer.readInteger(as: Int32.self) == 100)
        buffer.moveReaderIndex(forwardBy: 4) // lower bound

        // Decode all 100 elements
        for i in 0..<100 {
            let len = try #require(buffer.readInteger(as: Int32.self))
            var slice = try #require(buffer.readSlice(length: Int(len)))
            #expect(slice.readInteger(as: UInt8.self) == 0x01)
            let decoded = try JSONDecoder().decode(SampleModel.self, from: Data(slice.readableBytesView))
            #expect(decoded == SampleModel(name: "item-\(i)", count: i))
        }

        #expect(buffer.readableBytes == 0)
    }

    // MARK: - PostgresJSONBArray: through PostgresBindings

    @Test func jsonbArrayAppendToBindings() throws {
        let models = [SampleModel(name: "a", count: 1), SampleModel(name: "b", count: 2)]
        var bindings = PostgresBindings()
        try bindings.append(PostgresJSONBArray(models), context: .default)

        #expect(bindings.count == 1)
        #expect(bindings.metadata[0].dataType == .jsonbArray)
        #expect(bindings.metadata[0].format == .binary)

        var bytes = bindings.bytes
        let totalLength = try #require(bytes.readInteger(as: Int32.self))
        #expect(totalLength > 0)
        #expect(Int(totalLength) == bytes.readableBytes)
    }

    // MARK: - OID consistency

    @Test func jsonbArrayElementOIDInWireFormatMatchesJsonbType() throws {
        let wrapper = PostgresJSONBArray([SampleModel(name: "oid", count: 0)])
        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: .default)

        // Skip dimensions (4) and has-null (4)
        buffer.moveReaderIndex(forwardBy: 8)
        let writtenOID = try #require(buffer.readInteger(as: UInt32.self))
        #expect(writtenOID == PostgresDataType.jsonb.rawValue)
    }

    @Test func jsonbArrayTypeOIDIsArrayOfJsonb() {
        #expect(PostgresDataType.jsonb.arrayType == .jsonbArray)
        #expect(PostgresJSONBArray<SampleModel>.psqlType == PostgresDataType.jsonb.arrayType)
    }

    @Test func jsonbArrayElementTypeRoundTrips() {
        #expect(PostgresDataType.jsonbArray.elementType == .jsonb)
    }

    // MARK: - String interpolation: \(jsonb:)

    @Test func interpolationJSONBSingleValue() throws {
        let model = SampleModel(name: "interp", count: 5)
        let query: PostgresQuery = try "INSERT INTO t (data) VALUES (\(jsonb: model))"

        #expect(query.sql == "INSERT INTO t (data) VALUES ($1)")
        #expect(query.binds.count == 1)
        #expect(query.binds.metadata[0].dataType == .jsonb)
        #expect(query.binds.metadata[0].format == .binary)
    }

    @Test func interpolationJSONBArray() throws {
        let models = [SampleModel(name: "x", count: 1)]
        let query: PostgresQuery = try "INSERT INTO t (tags) VALUES (\(jsonb: models))"

        #expect(query.sql == "INSERT INTO t (tags) VALUES ($1)")
        #expect(query.binds.count == 1)
        #expect(query.binds.metadata[0].dataType == .jsonbArray)
    }

    @Test func interpolationJSONBNil() throws {
        let value: SampleModel? = nil
        let query: PostgresQuery = try "SELECT * FROM t WHERE data = \(jsonb: value)"

        #expect(query.sql == "SELECT * FROM t WHERE data = $1")
        #expect(query.binds.count == 1)
        #expect(query.binds.metadata[0].dataType == .null)
    }

    @Test func interpolationJSONBSome() throws {
        let value: SampleModel? = SampleModel(name: "opt", count: 3)
        let query: PostgresQuery = try "SELECT * FROM t WHERE data = \(jsonb: value)"

        #expect(query.sql == "SELECT * FROM t WHERE data = $1")
        #expect(query.binds.count == 1)
        #expect(query.binds.metadata[0].dataType == .jsonb)
    }

    @Test func interpolationMixedNativeAndJSONBBinds() throws {
        let model = SampleModel(name: "mixed", count: 10)
        let id: Int64 = 42
        let query: PostgresQuery = try "UPDATE t SET data = \(jsonb: model) WHERE id = \(id)"

        #expect(query.sql == "UPDATE t SET data = $1 WHERE id = $2")
        #expect(query.binds.count == 2)
        #expect(query.binds.metadata[0].dataType == .jsonb)
        #expect(query.binds.metadata[1].dataType == .int8)
    }

    @Test func interpolationMultipleJSONBBinds() throws {
        let a = SampleModel(name: "a", count: 1)
        let b = SampleModel(name: "b", count: 2)
        let query: PostgresQuery = try "INSERT INTO t (a, b) VALUES (\(jsonb: a), \(jsonb: b))"

        #expect(query.sql == "INSERT INTO t (a, b) VALUES ($1, $2)")
        #expect(query.binds.count == 2)
        #expect(query.binds.metadata[0].dataType == .jsonb)
        #expect(query.binds.metadata[1].dataType == .jsonb)
    }

    /// Verifies exact byte layout produced by interpolation, following the same
    /// pattern used in PostgresQueryTests.testStringInterpolationWithCustomJSONEncoder.
    @Test func interpolationJSONBExactBindingsBytes() throws {
        let model = SampleModel(name: "x", count: 0)
        let query: PostgresQuery = try "SELECT \(jsonb: model)"

        let jsonPayload = try JSONEncoder().encode(model)

        var expected = ByteBuffer()
        // encodeRaw: [Int32 length][UInt8 version][json bytes]
        expected.writeInteger(Int32(1 + jsonPayload.count))
        expected.writeInteger(UInt8(0x01))
        expected.writeData(jsonPayload)

        #expect(query.binds.bytes == expected)
    }

    // MARK: - Custom encoding context

    @Test func jsonbRespectsCustomEncoder() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let context = PostgresEncodingContext(jsonEncoder: encoder)

        let model = SampleModel(name: "ctx", count: 0)
        var buffer = ByteBuffer()
        try PostgresJSONB(model).encode(into: &buffer, context: context)

        buffer.moveReaderIndex(forwardBy: 1) // skip version byte
        let jsonString = buffer.readString(length: buffer.readableBytes)!
        // With sortedKeys, "count" comes before "name"
        #expect(jsonString.hasPrefix("{\"count\""))
    }

    @Test func jsonbArrayRespectsCustomEncoder() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let context = PostgresEncodingContext(jsonEncoder: encoder)

        let wrapper = PostgresJSONBArray([SampleModel(name: "ctx", count: 0)])
        var buffer = ByteBuffer()
        try wrapper.encode(into: &buffer, context: context)

        // Skip array header (20 bytes) + element length (4 bytes) + version byte (1 byte)
        buffer.moveReaderIndex(forwardBy: 25)
        let jsonString = buffer.readString(length: buffer.readableBytes)!
        #expect(jsonString.hasPrefix("{\"count\""))
    }

    @Test func customEncoderIsCalled() throws {
        final class TestEncoder: PostgresJSONEncoder {
            let encodeHits = ManagedAtomic(0)

            func encode<T>(_ value: T, into buffer: inout ByteBuffer) throws where T: Encodable {
                self.encodeHits.wrappingIncrement(ordering: .relaxed)
            }

            func encode<T>(_ value: T) throws -> Data where T: Encodable {
                preconditionFailure()
            }
        }

        let encoder = TestEncoder()
        var buffer = ByteBuffer()
        try PostgresJSONB(SampleModel(name: "test", count: 0)).encode(
            into: &buffer, context: .init(jsonEncoder: encoder)
        )
        #expect(encoder.encodeHits.load(ordering: .relaxed) == 1)
    }

    @Test func customEncoderIsCalledPerArrayElement() throws {
        final class TestEncoder: PostgresJSONEncoder {
            let encodeHits = ManagedAtomic(0)

            func encode<T>(_ value: T, into buffer: inout ByteBuffer) throws where T: Encodable {
                self.encodeHits.wrappingIncrement(ordering: .relaxed)
            }

            func encode<T>(_ value: T) throws -> Data where T: Encodable {
                preconditionFailure()
            }
        }

        let encoder = TestEncoder()
        let models = [
            SampleModel(name: "a", count: 1),
            SampleModel(name: "b", count: 2),
            SampleModel(name: "c", count: 3),
        ]
        var buffer = ByteBuffer()
        try PostgresJSONBArray(models).encode(
            into: &buffer, context: .init(jsonEncoder: encoder)
        )
        #expect(encoder.encodeHits.load(ordering: .relaxed) == 3)
    }

    // MARK: - String interpolation with custom context

    @Test func interpolationJSONBWithCustomContext() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let context = PostgresEncodingContext(jsonEncoder: encoder)

        let model = SampleModel(name: "ctx", count: 0)
        let query: PostgresQuery = try "SELECT \(jsonb: model, context: context)"

        #expect(query.binds.count == 1)
        #expect(query.binds.metadata[0].dataType == .jsonb)

        // Verify sortedKeys is applied by checking the raw bytes
        var bytes = query.binds.bytes
        let length = try #require(bytes.readInteger(as: Int32.self))
        #expect(bytes.readInteger(as: UInt8.self) == 0x01)
        let jsonSlice = try #require(bytes.readSlice(length: Int(length) - 1))
        let jsonString = String(buffer: jsonSlice)
        #expect(jsonString.hasPrefix("{\"count\""))
    }

    @Test func interpolationJSONBArrayWithCustomContext() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let context = PostgresEncodingContext(jsonEncoder: encoder)

        let models = [SampleModel(name: "ctx", count: 0)]
        let query: PostgresQuery = try "SELECT \(jsonb: models, context: context)"

        #expect(query.binds.count == 1)
        #expect(query.binds.metadata[0].dataType == .jsonbArray)
    }
}
