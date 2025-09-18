@testable import _ConnectionPoolModule
import Testing

@Suite struct TinyFastSequenceTests {
    @Test func testCountIsEmptyAndIterator() {
        var sequence = TinyFastSequence<Int>()
        #expect(sequence.count == 0)
        #expect(sequence.isEmpty == true)
        #expect(sequence.first == nil)
        #expect(Array(sequence) == [])
        sequence.append(1)
        #expect(sequence.count == 1)
        #expect(sequence.isEmpty == false)
        #expect(sequence.first == 1)
        #expect(Array(sequence) == [1])
        sequence.append(2)
        #expect(sequence.count == 2)
        #expect(sequence.isEmpty == false)
        #expect(sequence.first == 1)
        #expect(Array(sequence) == [1, 2])
        sequence.append(3)
        #expect(sequence.count == 3)
        #expect(sequence.isEmpty == false)
        #expect(sequence.first == 1)
        #expect(Array(sequence) == [1, 2, 3])
    }

    @Test func testReserveCapacityIsForwarded() {
        var emptySequence = TinyFastSequence<Int>()
        emptySequence.reserveCapacity(8)
        emptySequence.append(1)
        emptySequence.append(2)
        emptySequence.append(3)
        guard case .n(let array) = emptySequence.base else {
            Issue.record("Expected sequence to be backed by an array")
            return
        }
        #expect(array.capacity >= 8)

        var oneElemSequence = TinyFastSequence<Int>(element: 1)
        oneElemSequence.reserveCapacity(8)
        oneElemSequence.append(2)
        oneElemSequence.append(3)
        guard case .n(let array) = oneElemSequence.base else {
            Issue.record("Expected sequence to be backed by an array")
            return
        }
        #expect(array.capacity >= 8)

        var twoElemSequence = TinyFastSequence<Int>([1, 2])
        twoElemSequence.reserveCapacity(8)
        twoElemSequence.append(3)
        guard case .n(let array) = twoElemSequence.base else {
            Issue.record("Expected sequence to be backed by an array")
            return
        }
        #expect(array.capacity >= 8)

        var threeElemSequence = TinyFastSequence<Int>([1, 2, 3])
        threeElemSequence.reserveCapacity(8)
        guard case .n(let array) = threeElemSequence.base else {
            Issue.record("Expected sequence to be backed by an array")
            return
        }
        #expect(array.capacity >= 8)
    }

    @Test func testNewSequenceSlowPath() {
        let sequence = TinyFastSequence<UInt8>("AB".utf8)
        #expect(Array(sequence) == [UInt8(ascii: "A"), UInt8(ascii: "B")])
    }

    @Test func testSingleItem() {
        let sequence = TinyFastSequence<UInt8>("A".utf8)
        #expect(Array(sequence) == [UInt8(ascii: "A")])
    }

    @Test func testEmptyCollection() {
        let sequence = TinyFastSequence<UInt8>("".utf8)
        #expect(sequence.isEmpty == true)
        #expect(sequence.count == 0)
        #expect(Array(sequence) == [])
    }
}
