public protocol ConnectionPoolStatisticsReporter {
    func report(statistics: ConnectionPoolStatistics)
}
