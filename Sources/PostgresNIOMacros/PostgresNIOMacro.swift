import SwiftCompilerPlugin
import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros
import SwiftDiagnostics

public struct PostgresTypedQueryMacro: MemberMacro {
    public static func expansion(
        of node: AttributeSyntax,
        providingMembersOf declaration: some DeclGroupSyntax,
        in context: some MacroExpansionContext
    ) throws -> [DeclSyntax] {
        guard declaration.is(StructDeclSyntax.self) else {
            context.diagnose(Diagnostic(node: Syntax(node), message: PostgresNIODiagnostic.notAStruct))
            return []
        }

        guard let elements = node.argument?.as(TupleExprElementListSyntax.self)?
            .first?.as(TupleExprElementSyntax.self)?
            .expression.as(StringLiteralExprSyntax.self)?.segments else {
            // TODO: Be more specific about this error
            context.diagnose(Diagnostic(node: Syntax(node), message: PostgresNIODiagnostic.wrongArgument))
            return []
        }

        

        var outputTypes: [(String, String)] = []
        for tup in elements {
            if let expression = tup.as(ExpressionSegmentSyntax.self) {
                outputTypes.append(extractColumnTypes(expression))
            }
        }

        let rowStruct = try StructDeclSyntax("struct Row") {
            for outputType in outputTypes {
                MemberDeclListItemSyntax.init(decl: DeclSyntax(stringLiteral: "let \(outputType.0): \(outputType.1)"))
            }
            try InitializerDeclSyntax("init(from: PostgresRow) throws") {
                // TODO: (id, name) = try row.decode((Int, String).self, context: .default)
            }
        }

        return [
//            DeclSyntax(rowStruct)
        ]
    }

    /// Returns ("name", "String")
    private static func extractColumnTypes(_ node: ExpressionSegmentSyntax) -> (String, String) {
        let tupleElements = node.expressions
        guard tupleElements.count == 2 else {
            fatalError("Expected tuple with exactly two elements")
        }

        // First element needs to be the column name
        var iterator = tupleElements.makeIterator()
        guard let columnName = iterator.next()?.expression.as(StringLiteralExprSyntax.self)?
            .segments.first?.as(StringSegmentSyntax.self)?.content
            .text else {
            fatalError("Expected column name")
        }

        guard let columnType = iterator.next()?.expression.as(MemberAccessExprSyntax.self)?.base?.as(IdentifierExprSyntax.self)?.identifier.text else {
            fatalError("Expected column type")
        }
        return (columnName, columnType)
    }
}

@main
struct PostgresNIOMacros: CompilerPlugin {
    let providingMacros: [Macro.Type] = [
        PostgresTypedQueryMacro.self
    ]
}
