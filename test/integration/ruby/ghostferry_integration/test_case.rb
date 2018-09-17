require "logger"
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
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO
      @ghostferry = Ghostferry.new(ghostferry_main_path, logger: @logger)
    end

    def after_all
      @ghostferry.remove_binary
    end

    def before_setup
      @dbs = DbManager.new(logger: @logger)
      @dbs.reset_data

      @datawriter = DataWriter.new(@dbs.source_db_config, logger: @logger)
    end

    def after_teardown
      terminate_datawriter_and_ghostferry
    end

    ######################
    # Test Setup Helpers #
    ######################

    # This should be a no op if ghostferry and datawriter have already been
    # stopped.
    def terminate_datawriter_and_ghostferry
      @datawriter.stop
      @datawriter.join

      @ghostferry.stop_and_cleanup
      @datawriter = DataWriter.new(@dbs.source_db_config, logger: @logger)
    end

    # This is useful if we need to run Ghostferry multiple times in during a
    # single run, such as during an interrupt + resume cycle.
    def with_state_cleanup
      @datawriter = DataWriter.new(@dbs.source_db_config, logger: @logger)
      @ghostferry.reset_state
      yield
      terminate_datawriter_and_ghostferry
    end

    def start_datawriter_with_ghostferry(&on_write)
      @ghostferry.on_status(Ghostferry::Status::READY) do
        @datawriter.start(&on_write)
      end
    end

    def stop_datawriter_during_cutover
      @ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
        # At the start of the cutover phase, we have to set the database to
        # read-only. This is done by stopping the datawriter.
        @datawriter.stop
        @datawriter.join
      end
    end

    def use_datawriter(&on_write)
      start_datawriter_with_ghostferry(&on_write)
      stop_datawriter_during_cutover
    end

    def interrupt_ghostferry_when_some_batches_are_copied(batches: 2)
      batches_written = 0
      @ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
        batches_written += 1
        if batches_written >= batches
          @ghostferry.send_signal("TERM")
        end
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

    # Use this method to assert the validity of the structure of the dumped
    # state.
    #
    # To actually assert the validity of the data within the dumped state, you
    # have to do it manually.
    def assert_basic_fields_exist_in_dumped_state(dumped_state)
      refute dumped_state.nil?
      refute dumped_state["GhostferryVersion"].nil?
      refute dumped_state["LastKnownTableSchemaCache"].nil?
      refute dumped_state["LastSuccessfulPrimaryKeys"].nil?
      refute dumped_state["CompletedTables"].nil?
      refute dumped_state["LastWrittenBinlogPosition"].nil?
    end

    protected
    def ghostferry_main_path
      raise NotImplementedError
    end
  end
end
