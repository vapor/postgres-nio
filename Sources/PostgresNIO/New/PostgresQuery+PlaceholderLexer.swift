extension PostgresQuery {
    struct Placeholder {
        let position: Int
        let number: Int
    }

    enum PlaceholderLexer {
        /// Finds every top-level bind placeholder (`$1`, `$2`, ...) in the query,
        /// returning the UTF-8 offset of each placeholder's `$` and its number
        /// in order of finding. Placeholder-like text inside string literals, quoted
        /// identifiers, comments, and dollar-quoted strings is not matched.
        static func placeholders(in query: String) -> [Placeholder] {
            let utf8 = query.utf8
            var placeholders: [Placeholder] = []
            var index = utf8.startIndex

            while index < utf8.endIndex {
                switch utf8[index] {
                // String literal
                case UInt8(ascii: "'"):
                    index = self.consumeStringLiteral(utf8, from: utf8.index(after: index))

                // Bit/Hex string
                case UInt8(ascii: "B") where utf8[index...].starts(with: "B'".utf8),
                    UInt8(ascii: "b") where utf8[index...].starts(with: "b'".utf8),
                    UInt8(ascii: "X") where utf8[index...].starts(with: "X'".utf8),
                    UInt8(ascii: "x") where utf8[index...].starts(with: "x'".utf8):
                    index = self.consumeStringLiteral(utf8, from: utf8.index(index, offsetBy: 2))

                // Unicode string
                case UInt8(ascii: "U") where utf8[index...].starts(with: "U&'".utf8),
                    UInt8(ascii: "u") where utf8[index...].starts(with: "u&'".utf8):
                    index = self.consumeStringLiteral(utf8, from: utf8.index(index, offsetBy: 3))

                // Escape string
                case UInt8(ascii: "E") where utf8[index...].starts(with: "E'".utf8),
                    UInt8(ascii: "e") where utf8[index...].starts(with: "e'".utf8):
                    index = self.consumeEscapeString(utf8, from: utf8.index(index, offsetBy: 2))

                // Quoted identifier
                case UInt8(ascii: "\""):
                    index = self.consumeQuotedIdentifier(utf8, from: utf8.index(after: index))

                // Unicode identifier
                case UInt8(ascii: "U") where utf8[index...].starts(with: "U&\"".utf8),
                    UInt8(ascii: "u") where utf8[index...].starts(with: "u&\"".utf8):
                    index = self.consumeQuotedIdentifier(utf8, from: utf8.index(index, offsetBy: 3))

                // Line comment
                case UInt8(ascii: "-") where utf8[index...].starts(with: "--".utf8):
                    index = self.consumeLineComment(utf8, from: utf8.index(index, offsetBy: 2))

                // Block comment
                case UInt8(ascii: "/") where utf8[index...].starts(with: "/*".utf8):
                    index = self.consumeBlockComment(utf8, from: utf8.index(index, offsetBy: 2))

                // Placeholder
                case UInt8(ascii: "$") where utf8[index...].dropFirst().first?.isDigit == true:
                    let (newIndex, placeholder) = self.consumePlaceholder(utf8, from: index)
                    if let placeholder {
                        placeholders.append(placeholder)
                    }
                    index = newIndex

                // Dollar quoted string
                case UInt8(ascii: "$") where utf8[index...].dropFirst().first?.isDollarQuoteStart == true:
                    index = consumeDollarQuotedString(utf8, from: index)

                default:
                    index = utf8.index(after: index)
                }
            }

            return placeholders
        }

        static func consumeBlockComment(_ utf8: String.UTF8View, from start: String.Index) -> String.Index {
            var openComments = 1
            var index = start
            while index < utf8.endIndex, openComments > 0 {
                if utf8[index...].starts(with: "/*".utf8) {
                    openComments += 1
                    index = utf8.index(index, offsetBy: 2)
                } else if utf8[index...].starts(with: "*/".utf8) {
                    openComments -= 1
                    index = utf8.index(index, offsetBy: 2)
                } else {
                    index = utf8.index(after: index)
                }
            }
            return index
        }

        static func consumeDollarQuotedString(_ utf8: String.UTF8View, from start: String.Index) -> String.Index {
            if start != utf8.startIndex, utf8[utf8.index(before: start)].isIdentifierContinuation {
                // `foo$$bar$$` is part of an identifier, not a dollar quote
                return utf8.index(after: start)
            }

            var index = utf8.index(after: start)
            while index < utf8.endIndex, utf8[index].isDollarQuoteTagCharacter {
                index = utf8.index(after: index)
            }
            guard index < utf8.endIndex, utf8[index] == UInt8(ascii: "$") else {
                // invalid tag character or end of input before a second `$`
                return utf8.index(after: start)
            }
            index = utf8.index(after: index)

            let delimiter = utf8[start..<index]  // the whole "$tag$"
            while let candidate = utf8[index...].firstIndex(of: UInt8(ascii: "$")) {
                guard utf8[candidate...].starts(with: delimiter) else {
                    index = utf8.index(after: candidate)
                    continue
                }
                return utf8.index(candidate, offsetBy: delimiter.count)
            }
            return utf8.endIndex
        }

        static func consumePlaceholder(_ utf8: String.UTF8View, from start: String.Index) -> (String.Index, Placeholder?) {
            var index = start
            if index != utf8.startIndex {
                let before = utf8[utf8.index(before: index)]
                guard !before.isIdentifierContinuation else {
                    // `foo$1` is an identifier, not a placeholder
                    index = utf8.index(after: index)
                    return (index, nil)
                }
            }
            var placeholderEnd = utf8.index(after: index)
            guard utf8[placeholderEnd].isDigit else {
                index = utf8.index(after: index)
                return (index, nil)
            }
            var number = 0
            while placeholderEnd < utf8.endIndex, utf8[placeholderEnd].isDigit {
                number = number * 10 + Int(utf8[placeholderEnd] - UInt8(ascii: "0"))
                placeholderEnd = utf8.index(after: placeholderEnd)
            }
            return (
                placeholderEnd,
                .init(
                    position: utf8.distance(from: utf8.startIndex, to: index),
                    number: number
                )
            )
        }

        static func consumeLineComment(_ utf8: String.UTF8View, from start: String.Index) -> String.Index {
            var index = start
            while index < utf8.endIndex,
                utf8[index] != UInt8(ascii: "\n"),
                utf8[index] != UInt8(ascii: "\r")
            {
                index = utf8.index(after: index)
            }
            return index
        }

        // E'...'
        static func consumeEscapeString(_ utf8: String.UTF8View, from start: String.Index) -> String.Index {
            var index = start
            while index < utf8.endIndex {
                if utf8[index] == UInt8(ascii: "\\") {
                    index = utf8.index(index, offsetBy: 2, limitedBy: utf8.endIndex) ?? utf8.endIndex
                } else if utf8[index] == UInt8(ascii: "'") {
                    break
                } else {
                    index = utf8.index(after: index)
                }
            }
            guard index < utf8.endIndex else {
                // query finished without closing the string literal
                return utf8.endIndex
            }
            return utf8.index(after: index)
        }

        // '...'
        static func consumeStringLiteral(_ utf8: String.UTF8View, from start: String.Index) -> String.Index {
            var index = start
            while index < utf8.endIndex, utf8[index] != UInt8(ascii: "'") {
                index = utf8.index(after: index)
            }
            guard index < utf8.endIndex else {
                // query finished without closing the string literal
                return utf8.endIndex
            }
            return utf8.index(after: index)
        }

        // "..."
        static func consumeQuotedIdentifier(_ utf8: String.UTF8View, from start: String.Index) -> String.Index {
            var index = start
            while index < utf8.endIndex, utf8[index] != UInt8(ascii: "\"") {
                index = utf8.index(after: index)
            }
            guard index < utf8.endIndex else {
                // query finished without closing the quoted identifier
                return utf8.endIndex
            }
            return utf8.index(after: index)
        }
    }

    /// Returns `sql` with every top-level `$n` placeholder shifted by `offset`,
    /// skipping `$n`-looking identifiers that are not actually placeholders.
    static func renumberPlaceholders(in sql: String, by offset: Int) -> String {
        let placeholders = PlaceholderLexer.placeholders(in: sql)
        let utf8 = sql.utf8
        var consumed = utf8.startIndex
        var result = ""
        var consumedOffset = 0

        for placeholder in placeholders {
            let dollar = utf8.index(consumed, offsetBy: placeholder.position - consumedOffset)
            // copy everything since the last placeholder up to the '$'
            result.append(contentsOf: sql[consumed..<dollar])
            result.append(contentsOf: "$\(offset + placeholder.number)")
            // skip the '$' and the old digits in the source
            var index = utf8.index(after: dollar)
            var skipped = 1
            while index < utf8.endIndex, utf8[index].isDigit
            {
                index = utf8.index(after: index)
                skipped += 1
            }
            consumed = index
            consumedOffset = placeholder.position + skipped
        }
        result.append(contentsOf: sql[consumed...])

        return result
    }
}

extension UInt8 {
    /// `0-9`
    fileprivate var isDigit: Bool {
        UInt8(ascii: "0") <= self && self <= UInt8(ascii: "9")
    }

    /// A byte that continues an identifier, matching postgres' `scan.l`
    /// `ident_cont` class: `[A-Za-z\200-\377_0-9\$]`.
    fileprivate var isIdentifierContinuation: Bool {
        UInt8(ascii: "a") <= self && self <= UInt8(ascii: "z")
            || UInt8(ascii: "A") <= self && self <= UInt8(ascii: "Z")
            || self.isDigit
            || self == UInt8(ascii: "_")
            || self == UInt8(ascii: "$")
            || self >= 0x80
    }

    /// A byte that may follow the opening `$` of a dollar-quote delimiter:
    /// the tag's first character, matching postgres' `scan.l` `dolq_start`
    /// class (`[A-Za-z\200-\377_]`), or a second `$` for the `$$` form.
    fileprivate var isDollarQuoteStart: Bool {
        UInt8(ascii: "a") <= self && self <= UInt8(ascii: "z")
            || UInt8(ascii: "A") <= self && self <= UInt8(ascii: "Z")
            || self == UInt8(ascii: "_")
            || self == UInt8(ascii: "$")
            || self >= 0x80
    }

    /// A byte that continues a dollar-quote tag, matching postgres' `scan.l`
    /// `dolq_cont` class: `[A-Za-z\200-\377_0-9]`.
    fileprivate var isDollarQuoteTagCharacter: Bool {
        UInt8(ascii: "a") <= self && self <= UInt8(ascii: "z")
            || UInt8(ascii: "A") <= self && self <= UInt8(ascii: "Z")
            || self.isDigit
            || self == UInt8(ascii: "_")
            || self >= 0x80
    }
}
