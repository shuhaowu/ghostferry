require "json"
require "ghostferry_integration"

class TrivialIntegrationTests < GhostferryIntegration::TestCase
  def ghostferry_main_path
    "go/minimal.go"
  end

  def test_copy_data_without_any_writes_to_source
    @dbs.seed_simple_database_with_single_table
    @ghostferry.run
    assert_test_table_is_identical
  end

  def test_copy_data_with_writes_to_source
    use_datawriter

    @dbs.seed_simple_database_with_single_table

    @ghostferry.run
    assert_test_table_is_identical
  end

  def test_interrupt_resume_with_writes_to_source
    @dbs.seed_simple_database_with_single_table

    dumped_state = nil
    with_isolated_setup_and_teardown do
      use_datawriter

      batches_written = 0
      @ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
        batches_written += 1
        if batches_written >= 2
          @ghostferry.send_signal("TERM")
        end
      end

      dumped_state = @ghostferry.run_expecting_interrupt
      assert_basic_fields_exist_in_dumped_state(dumped_state)
    end

    # We want to write some data to the source database while Ghostferry is down
    # to verify that it is copied over.
    5.times do
      @datawriter.insert_data(@dbs.source)
      @datawriter.update_data(@dbs.source)
      @datawriter.delete_data(@dbs.source)
    end

    with_isolated_setup_and_teardown do
      use_datawriter
      @ghostferry.run(dumped_state)

      assert_test_table_is_identical
    end
  end

  def test_interrupt_resume_when_table_has_completed
    @dbs.seed_simple_database_with_single_table
    dumped_state = nil

    results = @dbs.source.query("SELECT COUNT(*) as cnt FROM #{GhostferryIntegration::DbManager::DEFAULT_FULL_TABLE_NAME}")
    rows = results.first["cnt"]

    with_isolated_setup_and_teardown do
      use_datawriter

      @ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
        @ghostferry.send_signal("TERM")
      end

      dumped_state = @ghostferry.run_expecting_interrupt
      assert_basic_fields_exist_in_dumped_state(dumped_state)
    end

    with_isolated_setup_and_teardown do
      use_datawriter
      @ghostferry.run(dumped_state)

      assert_test_table_is_identical
    end
  end
end
