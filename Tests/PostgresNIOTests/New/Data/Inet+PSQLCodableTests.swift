import Testing
import NIOCore
import PostgresNIO

@Suite struct Inet_PSQLCodableTests {

    // MARK: - Binary

    @Test func testBinaryIPv4RoundTrip() {
        let octets: (UInt8, UInt8, UInt8, UInt8) = (192, 0, 2, 1)
        let mask: UInt8 = 24
        let value = PostgresInet.ipv4(PostgresIPv4(value: octets), networkMask: mask)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(PostgresInet.psqlType == .inet)
        #expect(PostgresInet.psqlFormat == .binary)
        #expect(buffer.readableBytes == 8)
        #expect(buffer.getInteger(at: 0, as: UInt8.self) == 2)
        #expect(buffer.getInteger(at: 1, as: UInt8.self) == mask)
        #expect(buffer.getInteger(at: 2, as: UInt8.self) == 0)
        #expect(buffer.getInteger(at: 3, as: UInt8.self) == 4)
        #expect(buffer.getInteger(at: 4, as: UInt8.self) == octets.0)
        #expect(buffer.getInteger(at: 5, as: UInt8.self) == octets.1)
        #expect(buffer.getInteger(at: 6, as: UInt8.self) == octets.2)
        #expect(buffer.getInteger(at: 7, as: UInt8.self) == octets.3)

        var result: PostgresInet?
        #expect(throws: Never.self) {
            result = try PostgresInet(from: &buffer, type: .inet, format: .binary, context: .default)
        }
        #expect(value == result)
    }

    @Test func testBinaryIPv6RoundTrip() {
        let groups: (UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16) = (0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0001)
        let mask: UInt8 = 64
        let value = PostgresInet.ipv6(PostgresIPv6(value: groups), networkMask: mask)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(PostgresInet.psqlType == .inet)
        #expect(PostgresInet.psqlFormat == .binary)
        #expect(buffer.readableBytes == 20)
        #expect(buffer.getInteger(at: 0, as: UInt8.self) == 3)
        #expect(buffer.getInteger(at: 1, as: UInt8.self) == mask)
        #expect(buffer.getInteger(at: 2, as: UInt8.self) == 0)
        #expect(buffer.getInteger(at: 3, as: UInt8.self) == 16)
        #expect(buffer.getInteger(at: 4, as: UInt16.self) == groups.0)
        #expect(buffer.getInteger(at: 6, as: UInt16.self) == groups.1)
        #expect(buffer.getInteger(at: 8, as: UInt16.self) == groups.2)
        #expect(buffer.getInteger(at: 10, as: UInt16.self) == groups.3)
        #expect(buffer.getInteger(at: 12, as: UInt16.self) == groups.4)
        #expect(buffer.getInteger(at: 14, as: UInt16.self) == groups.5)
        #expect(buffer.getInteger(at: 16, as: UInt16.self) == groups.6)
        #expect(buffer.getInteger(at: 18, as: UInt16.self) == groups.7)

        var result: PostgresInet?
        #expect(throws: Never.self) {
            result = try PostgresInet(from: &buffer, type: .inet, format: .binary, context: .default)
        }
        #expect(value == result)
    }

    @Test func testBinaryIPv4NilNetmaskEncodesFullMask() {
        let value = PostgresInet.ipv4(PostgresIPv4(value: (192, 0, 2, 1)), networkMask: nil)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(buffer.getInteger(at: 1, as: UInt8.self) == 32)
    }

    @Test func testBinaryIPv6NilNetmaskEncodesFullMask() {
        let value = PostgresInet.ipv6(PostgresIPv6(value: (0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0001)), networkMask: nil)

        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .default)
        #expect(buffer.getInteger(at: 1, as: UInt8.self) == 128)
    }

    @Test func testBinaryDecodeInetInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(2)) // IP family
        buffer.writeInteger(UInt8(24)) // netmask length in bits
        buffer.writeInteger(UInt8(0)) // CIDR flag
        buffer.writeInteger(UInt8(4)) // address length in bytes
        buffer.writeInteger(UInt8(192)) // only 1 of 4 address bytes

        #expect(throws: PostgresDecodingError.Code.failure) {
            try PostgresInet(from: &buffer, type: .inet, format: .binary, context: .default)
        }
    }

    // MARK: - Text

    @Test func testTextDecodeInetThrows() {
        var buffer = ByteBuffer()
        buffer.writeString("192.0.2.1/24")

        #expect(throws: PostgresDecodingError.Code.failure) {
            try PostgresInet(from: &buffer, type: .inet, format: .text, context: .default)
        }
    }
}
