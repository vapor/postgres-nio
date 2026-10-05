import NIOCore
import Testing

@testable import PostgresNIO

@Suite struct PostgresLTree_PSQLCodableTests {
    @Test func testInitFromValues() throws {
        let ltree = PostgresLTree(labels: ["Top", "Science", "Astronomy"])
        #expect(ltree.labels == ["Top", "Science", "Astronomy"])
        #expect(ltree.description == "Top.Science.Astronomy")
    }

    @Test func testInitFromStringSplitsOnDots() {
        let ltree = PostgresLTree("Top.Science.Astronomy")
        #expect(ltree.labels == ["Top", "Science", "Astronomy"])
    }

    @Test func testSingleLabel() {
        let ltree = PostgresLTree("Top")
        #expect(ltree.labels == ["Top"])
        #expect(ltree.description == "Top")
    }

    @Test func testStringRoundTrip() {
        let paths = ["Top", "Top.Science", "Top.Science.Astronomy.Astrophysics", "a-b.c_d.e0"]
        for path in paths {
            let ltree = PostgresLTree(path)
            #expect(ltree.description == path)
        }
    }

    @Test func testParsing() {
        #expect(PostgresLTree("Top.Science.Astronomy").labels == ["Top", "Science", "Astronomy"])
        #expect(PostgresLTree("Top").labels == ["Top"])
        #expect(PostgresLTree("").labels == [])

        #expect(PostgresLTree("a..b").labels == ["a", "b"])
        #expect(PostgresLTree("a.b.").labels == ["a", "b"])
        #expect(PostgresLTree(".a.b").labels == ["a", "b"])
    }

    @Test func testEmptyPath() throws {
        let empty = PostgresLTree(labels: [])
        #expect(empty.description == "")

        var buffer = ByteBuffer()
        empty.encode(into: &buffer, context: .default)
        #expect(buffer.readableBytes == 0)

        let decoded = try PostgresLTree(from: &buffer, type: .text, format: .text, context: .default)
        #expect(decoded == empty)
        #expect(decoded.labels == [])
    }

    @Test func testAppend() throws {
        var ltree = PostgresLTree(labels: ["Top", "Science"])
        ltree.append("Astronomy")
        #expect(ltree.labels == ["Top", "Science", "Astronomy"])
        #expect(ltree.description == "Top.Science.Astronomy")
    }

    @Test func testPop() throws {
        var ltree = PostgresLTree(labels: ["Top", "Science", "Astronomy"])
        #expect(ltree.popLast() == "Astronomy")
        #expect(ltree.labels == ["Top", "Science"])
        #expect(ltree.popLast() == "Science")
        #expect(ltree.popLast() == "Top")
        #expect(ltree.popLast() == nil)
        #expect(ltree.labels == [])
    }

    @Test func testEncodeAsText() throws {
        let ltree = PostgresLTree(labels: ["Top", "Science", "Astronomy"])

        #expect(ltree.psqlType == .null)
        #expect(ltree.psqlFormat == .text)
        #expect(ltree.psqlTypeName == "ltree")

        var buffer = ByteBuffer()
        ltree.encode(into: &buffer, context: .default)
        #expect(buffer.getString(at: buffer.readerIndex, length: buffer.readableBytes) == "Top.Science.Astronomy")
    }

    @Test func testEncodeDecodeRoundTripAsText() throws {
        let ltree = PostgresLTree(labels: ["Top", "Science", "Astronomy"])

        var buffer = ByteBuffer()
        ltree.encode(into: &buffer, context: .default)

        let decoded = try PostgresLTree(from: &buffer, type: .text, format: .text, context: .default)
        #expect(decoded == ltree)
    }

    @Test func testDecodeFromBinary() throws {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int8(1))
        buffer.writeString("Top.Science.Astronomy")

        let ltree = try PostgresLTree(from: &buffer, type: .text, format: .binary, context: .default)
        #expect(ltree.labels == ["Top", "Science", "Astronomy"])
    }

    @Test func testDecodeFromText() throws {
        var buffer = ByteBuffer()
        buffer.writeString("Top.Science.Astronomy")

        let ltree = try PostgresLTree(from: &buffer, type: .text, format: .text, context: .default)
        #expect(ltree.labels == ["Top", "Science", "Astronomy"])
    }

    @Test func testDecodeFailureOnUnknownBinaryVersion() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int8(2))
        buffer.writeString("Top.Science.Astronomy")

        let error = #expect(throws: PostgresDecodingError.Code.self) {
            try PostgresLTree(from: &buffer, type: .text, format: .binary, context: .default)
        }
        #expect(error == .failure)
    }
}
