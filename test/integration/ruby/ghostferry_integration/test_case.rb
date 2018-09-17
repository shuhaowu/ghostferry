require "minitest"
require "minitest/hooks/test"

module GhostferryIntegration
  class TestCase < Minitest::Test
    include Minitest::Hooks
    include GhostferryIntegration

    ##############
    # Test Hooks #
    ##############

    def before_all
      @ghostferry = Ghostferry.new(ghostferry_main_path)
    end

    def after_all
      @ghostferry.remove_binary
    end

    def before_setup
      @dbs = DbManager.new
      @dbs.reset_data

      @datawriter = DataWriter.new(@dbs.source_db_config)
    end

    def after_teardown
      # Will be a no op if already stopped or not started
      @datawriter.stop
      @datawriter.join
    end

    ######################
    # Test Setup Helpers #
    ######################

    def use_datawriter
      @ghostferry.on_status(Ghostferry::Status::READY) do
        @datawriter.start
      end

      @ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
        # At the start of the cutover phase, we have to set the database to
        # read-only. This is done by stopping the datawriter.
        @datawriter.stop
        @datawriter.join
      end
    end

    #####################
    # Assertion Helpers #
    #####################

    def assert_test_table_is_identical
      source, target = @dbs.source_and_target_table_metrics

      assert source[DbManager::DEFAULT_FULL_TABLE_NAME][:row_count] > 0
      assert target[DbManager::DEFAULT_FULL_TABLE_NAME][:row_count] > 0

      assert_equal(
        source[DbManager::DEFAULT_FULL_TABLE_NAME][:checksum],
        target[DbManager::DEFAULT_FULL_TABLE_NAME][:checksum],
      )

      assert_equal(
        source[DbManager::DEFAULT_FULL_TABLE_NAME][:sample_row],
        target[DbManager::DEFAULT_FULL_TABLE_NAME][:sample_row],
      )
    end

    protected
    def ghostferry_main_path
      raise NotImplementedError
    end
  end
end
