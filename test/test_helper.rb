# frozen_string_literal: true

if ENV['COVERAGE'] == '1'
  require 'simplecov'
  require 'simplecov-lcov'

  SimpleCov.command_name(ENV['SIMPLECOV_COMMAND_NAME'] || "run-#{Process.pid}")
  SimpleCov.coverage_dir('coverage')
  SimpleCov.use_merging true

  SimpleCov::Formatter::LcovFormatter.config.report_with_single_file = true
  SimpleCov::Formatter::LcovFormatter.config do |c|
    c.output_directory = 'coverage'
    c.lcov_file_name = 'lcov.info'
  end
  SimpleCov.formatter = SimpleCov::Formatter::LcovFormatter

  SimpleCov.start do
    enable_coverage :branch
    add_filter '/test/'
    add_filter '/vendor/'
  end
end

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))

require 'channel_rb'
require 'minitest/autorun'
require 'timeout'

Minitest.parallel_executor = Minitest::Parallel::Executor.new(1)
