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
      if ENV["DEBUG"] == "1"
        @logger.level = Logger::DEBUG
      else
        @logger.level = Logger::INFO
      end
      @ghostferry = Ghostferry.new(ghostferry_main_path, logger: @logger)
    end

    def after_all
      @ghostferry.remove_binary
    end

    def before_setup
      @dbs = DbManager.new(logger: @logger)
      @dbs.reset_data

      setup_ghostferry_datawriter
    end

    def after_teardown
      teardown_ghostferry_datawriter
    end

    ######################
    # Test Setup Helpers #
    ######################

    # If multiple Ghostferry runs are needed within a single test, such as in
    # the case of interrupt/resume testing, we will need to wrap each
    # @ghostferry.run within a block for this method.
    #
    # This method doesn't destroy the database state like before_setup and
    # after_teardown does.
    def with_isolated_setup_and_teardown
      setup_ghostferry_datawriter
      yield
      teardown_ghostferry_datawriter
    end

    # This setup the datawriter to start when Ghostferry start and stop when
    # cutover is about to take place.
    #
    # The on_write block is called everytime the datawriter writes a row with
    # the argument op, id.
    #
    # op: "INSERT"/"UPDATE"/"DELETE"
    # id: the primary id of the row inserted
    def use_datawriter(&on_write)
      start_datawriter_with_ghostferry(&on_write)
      stop_datawriter_during_cutover
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
      refute dumped_state["CopyStage"].nil?
      refute dumped_state["CopyStage"]["LastSuccessfulPrimaryKeys"].nil?
      refute dumped_state["CopyStage"]["CompletedTables"].nil?
      refute dumped_state["CopyStage"]["LastWrittenBinlogPosition"].nil?
    end

    def assert_verifier_stage_exists_in_dumped_state(dumped_state)
      refute dumped_state["VerifierStage"].nil?
      refute dumped_state["VerifierStage"]["LastSuccessfulPrimaryKeys"].nil?
      refute dumped_state["VerifierStage"]["CompletedTables"].nil?
      refute dumped_state["VerifierStage"]["LastWrittenBinlogPosition"].nil?
      refute dumped_state["VerifierStage"]["ReverifyStore"].nil?
      refute dumped_state["VerifierStage"]["ReverifyStoreCount"].nil?
      count = 0
      dumped_state["VerifierStage"]["ReverifyStore"].each do |table, pks|
        count += pks.length
      end
      assert_equal count, dumped_state["VerifierStage"]["ReverifyStoreCount"]
    end

    protected
    def ghostferry_main_path
      raise NotImplementedError
    end

    private

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

    def setup_ghostferry_datawriter
      @ghostferry.reset_state
      @datawriter = DataWriter.new(@dbs.source_db_config, logger: @logger)
    end

    # This should be a no op if ghostferry and datawriter have already been
    # stopped.
    def teardown_ghostferry_datawriter
      @datawriter.stop
      @datawriter.join

      @ghostferry.stop_and_cleanup
    end
  end
end
