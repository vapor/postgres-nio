@attached(member)
macro Query(_ query: PostgresMacroQuery) = #externalMacro(
    module: "PostgresNIOMacros",
    type: "PostgresTypedQueryMacro"
)
