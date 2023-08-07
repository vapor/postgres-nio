#if swift(>=5.8)

@_documentation(visibility: internal) @_exported import NIO
@_documentation(visibility: internal) @_exported import NIOSSL
@_documentation(visibility: internal) @_exported import struct Logging.Logger

#else

// TODO: Remove this with the next major release!
@_exported import NIO
@_exported import NIOSSL
@_exported import struct Logging.Logger

#endif
