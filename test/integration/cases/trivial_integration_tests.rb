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

  def test_iterative_verifier_succeeds_in_normal_run
    use_datawriter
    @dbs.seed_simple_database_with_single_table

    verified = false
    @ghostferry.on_status(Ghostferry::Status::VERIFIED) do |num_failed, *incorrect_tables|
      verified = true

      assert_equal "0", num_failed
      assert_equal 0, incorrect_tables.length
    end

    @ghostferry.run_with_iterative_verifier_enabled
    assert verified
    assert_test_table_is_identical
  end

  def test_iterative_verifier_fails_if_binlog_streamer_incorrectly_copies_data
    use_datawriter
    @dbs.seed_simple_database_with_single_table

    # To fake that Ghostferry actually screwed up copying data, we first choose
    # a row and update it during ROW_COPY_COMPLETED. This row will be picked up
    # by the binlog streamer and also be added into the reverification queue.
    #
    # Then, during VERIFY_BEFORE_CUTOVER, at which point the binlog streaming
    # has stopped, we delete this row. This should cause the verification to
    # fail if this test is to be passed.

    table_name = GhostferryIntegration::DbManager::DEFAULT_FULL_TABLE_NAME

    chosen_id = 0
    verified = false
    @ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      result = @dbs.source.query("SELECT id FROM #{table_name} ORDER BY id LIMIT 1")
      chosen_id = result.first["id"]

      refute chosen_id == 0
      @dbs.source.query("UPDATE #{table_name} SET data = 'something' WHERE id = #{chosen_id}")
    end

    @ghostferry.on_status(Ghostferry::Status::VERIFY_DURING_CUTOVER) do
      refute chosen_id == 0
      @dbs.source.query("DELETE FROM #{table_name} WHERE id = #{chosen_id}")
    end

    @ghostferry.on_status(Ghostferry::Status::VERIFIED) do |num_failed, *incorrect_tables|
      verified = true

      assert_equal "1", num_failed
      assert_equal ["gftest.test_table_1"], incorrect_tables
    end

    @ghostferry.run_with_iterative_verifier_enabled
    assert verified
  end
end
