import NIOCore
import Testing

@testable import PostgresNIO

@Suite struct PostgresQueryRenumberingTests {
    @Test func renumbersTopLevelPlaceholders() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "SELECT * FROM t WHERE a = $1 AND b = $2", by: 2)
                == "SELECT * FROM t WHERE a = $3 AND b = $4"
        )
    }

    @Test func offsetZeroLeavesQueryUnchanged() {
        let sql = "SELECT * FROM t WHERE a = $1 AND note = 'costs $1'"
        #expect(PostgresQuery.renumberPlaceholders(in: sql, by: 0) == sql)
    }

    @Test func renumbersPlaceholderAtStartAndEndOfString() {
        #expect(PostgresQuery.renumberPlaceholders(in: "$1 = $2", by: 1) == "$2 = $3")
        #expect(PostgresQuery.renumberPlaceholders(in: "a = $1", by: 9) == "a = $10")
    }

    @Test func renumbersMultiDigitPlaceholders() {
        #expect(PostgresQuery.renumberPlaceholders(in: "($9, $10, $12)", by: 1) == "($10, $11, $13)")
    }

    @Test func queryWithoutPlaceholdersIsUnchanged() {
        let sql = "SELECT * FROM t WHERE deleted_at IS NULL"
        #expect(PostgresQuery.renumberPlaceholders(in: sql, by: 5) == sql)
    }

    @Test func stringLiteral() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "note = 'costs $1' AND a = $1", by: 1)
                == "note = 'costs $1' AND a = $2"
        )
    }

    @Test func stringLiteralWithEscapedQuote() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "note = 'it''s $1 worth' AND a = $1", by: 1)
                == "note = 'it''s $1 worth' AND a = $2"
        )
    }

    @Test func escapeStringConstant() {
        // In E'...' a backslash escapes the quote, so \' does not end the string
        #expect(
            PostgresQuery.renumberPlaceholders(in: #"note = E'\' $1' AND a = $1"#, by: 1)
                == #"note = E'\' $1' AND a = $2"#
        )
    }

    @Test func dollarQuotedString() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "body = $$ $1 $$ AND a = $1", by: 1)
                == "body = $$ $1 $$ AND a = $2"
        )
    }

    @Test func taggedDollarQuotedString() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "body = $fn$ SELECT $$ $1 $$ $fn$ AND a = $1", by: 1)
                == "body = $fn$ SELECT $$ $1 $$ $fn$ AND a = $2"
        )
    }

    @Test func bitString() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "note = B'1 $1' AND a = $1", by: 1)
                == "note = B'1 $1' AND a = $2"
        )
    }

    @Test func quotedIdentifier() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: #""weird$1name" = $1"#, by: 1)
                == #""weird$1name" = $2"#
        )
    }

    @Test func lineComment() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "a = $1 -- not $2 here\nAND b = $2", by: 1)
                == "a = $2 -- not $2 here\nAND b = $3"
        )
    }

    @Test func blockComment() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "/* $1 */ a = $1", by: 1)
                == "/* $1 */ a = $2"
        )
    }

    @Test func nestedBlockComment() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "/* $1 /* $2 */ still $3 */ a = $1", by: 1)
                == "/* $1 /* $2 */ still $3 */ a = $2"
        )
    }

    @Test func dollarInsideUnquotedIdentifier() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "foo$1 = $1", by: 1)
                == "foo$1 = $2"
        )
    }

    @Test func loneDollarSign() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "a $ b = $1", by: 1)
                == "a $ b = $2"
        )
    }

    @Test func unterminatedStringIsLeftAlone() {
        let sql = "note = 'oops $1"
        #expect(PostgresQuery.renumberPlaceholders(in: sql, by: 1) == sql)
    }

    @Test func functionBody() {
        let sql = "CREATE FUNCTION add(int, int) RETURNS int AS $$ SELECT $1 + $2 $$ LANGUAGE sql; SELECT add(a, $1)"
        #expect(
            PostgresQuery.renumberPlaceholders(in: sql, by: 3)
                == "CREATE FUNCTION add(int, int) RETURNS int AS $$ SELECT $1 + $2 $$ LANGUAGE sql; SELECT add(a, $4)"
        )
    }

    @Test func trailingDollarSign() {
        // `$` as the very last byte is not a placeholder
        let sql = "a = $"
        #expect(PostgresQuery.renumberPlaceholders(in: sql, by: 1) == sql)
    }

    @Test func unterminatedEscapeStringWithTrailingBackslash() {
        let sql = #"note = E'\"#
        #expect(PostgresQuery.renumberPlaceholders(in: sql, by: 1) == sql)
    }

    @Test func emptyLineComment() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "--\n$1 = a", by: 1)
                == "--\n$2 = a"
        )
    }

    @Test func emptyBlockComment() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "/**/ $1", by: 1)
                == "/**/ $2"
        )
    }

    @Test func dollarFollowedByOperator() {
        // `$+` cannot open a dollar quote: `+` is not a valid tag character
        #expect(
            PostgresQuery.renumberPlaceholders(in: "a $+ b = $1", by: 1)
                == "a $+ b = $2"
        )
    }

    @Test func unicodeString() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "note = U&'x $1' AND a = $1", by: 1)
                == "note = U&'x $1' AND a = $2"
        )
    }

    @Test func emptyUnicodeString() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "note = U&'' AND a = $1", by: 1)
                == "note = U&'' AND a = $2"
        )
    }

    @Test func invalidDollarQuoteTag() {
        // `$x y$` is not a dollar quote — tags cannot contain spaces
        #expect(
            PostgresQuery.renumberPlaceholders(in: "a $x y$ = $1", by: 1)
                == "a $x y$ = $2"
        )
    }

    @Test func dollarQuoteAfterIdentifier() {
        // `foo$$bar$$` is a single identifier, not a dollar quote
        #expect(
            PostgresQuery.renumberPlaceholders(in: "foo$$bar$$ = $1", by: 1)
                == "foo$$bar$$ = $2"
        )
    }

    @Test func nonASCIIDollarQuoteTag() {
        #expect(
            PostgresQuery.renumberPlaceholders(in: "body = $é$ $1 $é$ AND a = $1", by: 1)
                == "body = $é$ $1 $é$ AND a = $2"
        )
    }

    @Test func unsafeSQLFragmentComposes() {
        var binds = PostgresBindings()
        binds.append(1, context: .default)
        let fragment = PostgresQuery(unsafeSQL: "a = $1", binds: binds)
        let b = 2

        let query: PostgresQuery = "SELECT * FROM t WHERE b = \(b) AND \(fragment)"

        #expect(query.sql == "SELECT * FROM t WHERE b = $1 AND a = $2")

        var expected = ByteBuffer()
        for value in [b, 1] {
            expected.writeInteger(UInt32(8))
            expected.writeInteger(value)
        }
        #expect(query.binds.bytes == expected)
    }
}
