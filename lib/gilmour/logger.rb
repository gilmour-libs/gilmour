require 'logger'

class GilmourLogger
  LoggerLevels = {
    unknown: Logger::UNKNOWN,
    fatal: Logger::FATAL,
    error: Logger::ERROR,
    warn: Logger::WARN,
    info: Logger::INFO,
    debug: Logger::DEBUG
  }

  def initialize
    @logger = Logger.new(STDERR)
    log_level =  ENV["LOG_LEVEL"] ? ENV["LOG_LEVEL"].to_sym : :warn
    @logger.level = LoggerLevels[log_level] || Logger::WARN
  end

  private
  def method_missing(method, *args, &block)
    $stderr.puts "Fwding GLogger: #{method} - #{args}"
    @logger.send(method, *args, &block)
  end
end
