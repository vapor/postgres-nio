@testable import _ConnectionPoolModule
import XCTest

final class TinyFastSequenceTests: XCTestCase {
    func testCountIsEmptyAndIterator() async {
        var sequence = TinyFastSequence<Int>()
        XCTAssertEqual(sequence.count, 0)
        XCTAssertEqual(sequence.isEmpty, true)
        XCTAssertEqual(sequence.first, nil)
        XCTAssertEqual(Array(sequence), [])
        sequence.append(1)
        XCTAssertEqual(sequence.count, 1)
        XCTAssertEqual(sequence.isEmpty, false)
        XCTAssertEqual(sequence.first, 1)
        XCTAssertEqual(Array(sequence), [1])
        sequence.append(2)
        XCTAssertEqual(sequence.count, 2)
        XCTAssertEqual(sequence.isEmpty, false)
        XCTAssertEqual(sequence.first, 1)
        XCTAssertEqual(Array(sequence), [1, 2])
        sequence.append(3)
        XCTAssertEqual(sequence.count, 3)
        XCTAssertEqual(sequence.isEmpty, false)
        XCTAssertEqual(sequence.first, 1)
        XCTAssertEqual(Array(sequence), [1, 2, 3])
    }

    func testReserveCapacityIsForwarded() {
        var emptySequence = TinyFastSequence<Int>()
        emptySequence.reserveCapacity(8)
        emptySequence.append(1)
        emptySequence.append(2)
        emptySequence.append(3)
        guard case .n(let array) = emptySequence.base else {
            return XCTFail("Expected sequence to be backed by an array")
        }
        XCTAssertEqual(array.capacity, 8)

        var oneElemSequence = TinyFastSequence<Int>(element: 1)
        oneElemSequence.reserveCapacity(8)
        oneElemSequence.append(2)
        oneElemSequence.append(3)
        guard case .n(let array) = oneElemSequence.base else {
            return XCTFail("Expected sequence to be backed by an array")
        }
        XCTAssertEqual(array.capacity, 8)

        var twoElemSequence = TinyFastSequence<Int>([1, 2])
        twoElemSequence.reserveCapacity(8)
        guard case .n(let array) = twoElemSequence.base else {
            return XCTFail("Expected sequence to be backed by an array")
        }
        XCTAssertEqual(array.capacity, 8)
    }

    func testNewSequenceSlowPath() {
        let sequence = TinyFastSequence<UInt8>("AB".utf8)
        XCTAssertEqual(Array(sequence), [UInt8(ascii: "A"), UInt8(ascii: "B")])
    }

    func testSingleItem() {
        let sequence = TinyFastSequence<UInt8>("A".utf8)
        XCTAssertEqual(Array(sequence), [UInt8(ascii: "A")])
    }

    func testEmptyCollection() {
        let sequence = TinyFastSequence<UInt8>("".utf8)
        XCTAssertTrue(sequence.isEmpty)
        XCTAssertEqual(sequence.count, 0)
        XCTAssertEqual(Array(sequence), [])
    }
}
