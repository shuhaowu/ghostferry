require "thread"

# Thread.abort_on_exception = true

module GhostferryIntegration
  # For random data generation
  ALPHANUMERICS = ("0".."9").to_a + ("a".."z").to_a + ("A".."Z").to_a

  def self.rand_data(length: 32)
    ALPHANUMERICS.sample(32).join("") + "👻⛴️"
  end
end

require_relative "ghostferry_integration/db_manager"
require_relative "ghostferry_integration/data_writer"
require_relative "ghostferry_integration/ghostferry"
require_relative "ghostferry_integration/test_case"
