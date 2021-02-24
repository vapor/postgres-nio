import Logging

extension Logger {
    static var psqlTest: Logger {
        var logger = Logger(label: "psql.test")
        logger.logLevel = .info
        return logger
    }
}
