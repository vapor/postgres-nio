//
//  File.swift
//  
//
//  Created by Fabian Fett on 07.01.21.
//

import Logging

extension Logger {
    static var psqlTest: Logger {
        var logger = Logger(label: "psql.test")
        logger.logLevel = .info
        return logger
    }
}
