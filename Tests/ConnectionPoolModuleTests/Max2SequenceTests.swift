@testable import _ConnectionPoolModule
import XCTest

final class Max2SequenceTests: XCTestCase {
    func testCountAndIsEmpty() async {
        var sequence = Max2Sequence<Int>()
        XCTAssertEqual(sequence.count, 0)
        XCTAssertEqual(sequence.isEmpty, true)
        sequence.append(1)
        XCTAssertEqual(sequence.count, 1)
        XCTAssertEqual(sequence.isEmpty, false)
        sequence.append(2)
        XCTAssertEqual(sequence.count, 2)
        XCTAssertEqual(sequence.isEmpty, false)
    }

    func testOptionalInitializer() {
        let emptySequence = Max2Sequence<Int>(nil, nil)
        XCTAssertEqual(emptySequence.count, 0)
        XCTAssertEqual(emptySequence.isEmpty, true)
        var emptySequenceIterator = emptySequence.makeIterator()
        XCTAssertNil(emptySequenceIterator.next())
        XCTAssertNil(emptySequenceIterator.next())
        XCTAssertNil(emptySequenceIterator.next())

        let oneElemSequence1 = Max2Sequence<Int>(1, nil)
        XCTAssertEqual(oneElemSequence1.count, 1)
        XCTAssertEqual(oneElemSequence1.isEmpty, false)
        var oneElemSequence1Iterator = oneElemSequence1.makeIterator()
        XCTAssertEqual(oneElemSequence1Iterator.next(), 1)
        XCTAssertNil(oneElemSequence1Iterator.next())
        XCTAssertNil(oneElemSequence1Iterator.next())

        let oneElemSequence2 = Max2Sequence<Int>(nil, 2)
        XCTAssertEqual(oneElemSequence2.count, 1)
        XCTAssertEqual(oneElemSequence2.isEmpty, false)
        var oneElemSequence2Iterator = oneElemSequence2.makeIterator()
        XCTAssertEqual(oneElemSequence2Iterator.next(), 2)
        XCTAssertNil(oneElemSequence2Iterator.next())
        XCTAssertNil(oneElemSequence2Iterator.next())

        let twoElemSequence = Max2Sequence<Int>(1, 2)
        XCTAssertEqual(twoElemSequence.count, 2)
        XCTAssertEqual(twoElemSequence.isEmpty, false)
        var twoElemSequenceIterator = twoElemSequence.makeIterator()
        XCTAssertEqual(twoElemSequenceIterator.next(), 1)
        XCTAssertEqual(twoElemSequenceIterator.next(), 2)
        XCTAssertNil(twoElemSequenceIterator.next())
    }

    func testMap() {
        let twoElemSequence = Max2Sequence<Int>(1, 2).map({ "\($0)" })
        XCTAssertEqual(twoElemSequence.count, 2)
        XCTAssertEqual(twoElemSequence.isEmpty, false)
        var twoElemSequenceIterator = twoElemSequence.makeIterator()
        XCTAssertEqual(twoElemSequenceIterator.next(), "1")
        XCTAssertEqual(twoElemSequenceIterator.next(), "2")
        XCTAssertNil(twoElemSequenceIterator.next())
    }
}
