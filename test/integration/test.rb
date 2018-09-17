require "minitest/autorun"

ruby_path = File.join(File.absolute_path(File.dirname(__FILE__)), "ruby")
$LOAD_PATH.unshift(ruby_path) unless $LOAD_PATH.include?(ruby_path)

require_relative "cases/trivial_integration_tests"
