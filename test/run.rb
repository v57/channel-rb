# frozen_string_literal: true

require_relative 'test_helper'

Dir[File.expand_path('unit/*_test.rb', __dir__)].sort.each { |file| require file }
Dir[File.expand_path('integration/*_test.rb', __dir__)].sort.each { |file| require file }
