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

    @ghostferry.run(environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
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

    @ghostferry.run(environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
    assert verified
  end

  def test_iterative_verifier_interrupt_resume
    @dbs.seed_simple_database_with_single_table

    dumped_state = nil
    with_isolated_setup_and_teardown do
      use_datawriter
      batches_verified = 0
      @ghostferry.on_status(Ghostferry::Status::VERIFY_ROW_EVENT) do
        batches_verified += 1
        if batches_verified >= 2
          @ghostferry.send_signal("TERM")
        end
      end

      dumped_state = @ghostferry.run_expecting_interrupt(environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
      assert_basic_fields_exist_in_dumped_state(dumped_state)
      assert_verifier_stage_exists_in_dumped_state(dumped_state)
    end

    # We want to write some data to the source database while Ghostferry is down
    # to verify that it is copied over.
    5.times do
      @datawriter.insert_data(@dbs.source)
      @datawriter.update_data(@dbs.source)
      @datawriter.delete_data(@dbs.source)
    end

    verified = false

    with_isolated_setup_and_teardown do
      use_datawriter

      @ghostferry.on_status(Ghostferry::Status::VERIFIED) do |num_failed, *incorrect_tables|
        verified = true

        assert_equal "0", num_failed
        assert_equal [], incorrect_tables
      end

      @ghostferry.run(dumped_state, environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})

      assert_test_table_is_identical
    end

    assert verified
  end

  def test_iterative_verifier_fails_if_target_row_changed_during_interrupt
    @dbs.seed_simple_database_with_single_table

    dumped_state = nil
    with_isolated_setup_and_teardown do
      use_datawriter
      batches_verified = 0
      @ghostferry.on_status(Ghostferry::Status::VERIFY_ROW_EVENT) do
        batches_verified += 1
        if batches_verified >= 2
          @ghostferry.send_signal("TERM")
        end
      end

      dumped_state = @ghostferry.run_expecting_interrupt(environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
      assert_basic_fields_exist_in_dumped_state(dumped_state)
      assert_verifier_stage_exists_in_dumped_state(dumped_state)
    end

    # 1. Select a row from the target and update it.
    # 2. Update the same row on the source will mean that the binlog will not
    #    successfully update on the target as the WHERE condition generated by
    #    the binlog streamer will not match any rows on the target.
    # 3. Effectively, this means a binlog event will not be successfully
    #    replicated to the target database.
    # 4. Verification should thus fail.
    results = @dbs.target.query("SELECT id FROM #{DbManager::DEFAULT_FULL_TABLE_NAME} ORDER BY id ASC LIMIT 1")
    id = results.first["id"]
    @dbs.target.query("UPDATE #{DbManager::DEFAULT_FULL_TABLE_NAME} SET data = 'modified_target' WHERE id = #{id}")
    @dbs.source.query("UPDATE #{DbManager::DEFAULT_FULL_TABLE_NAME} SET data = 'modified_source' WHERE id = #{id}")

    verified = false

    with_isolated_setup_and_teardown do
      # Do not use the DataWriter in this resume as it could change the row
      # selected above. Doing so will likely generate a reverify entry during
      # the normal time at which the IterativeVerifier runs and can cause the
      # test to pass when it is not supposed to (if the IterativeVerifier did
      # not pickup the binlog entries during the Ghostferry downtime).

      @ghostferry.on_status(Ghostferry::Status::VERIFIED) do |num_failed, *incorrect_tables|
        verified = true

        assert_equal "1", num_failed
        assert_equal ["gftest.test_table_1"], incorrect_tables
      end

      @ghostferry.run(dumped_state, environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
    end

    assert verified
  end

  def test_iterative_verifier_saves_reverify_store_and_verifies_rows_changed_in_reverify_queue_on_resume
    @dbs.seed_simple_database_with_single_table

    dumped_state = nil
    updated_id = 0
    with_isolated_setup_and_teardown do
      # We do not use the DataWriter in this test because we want to manually
      # inject a row into the reverification queue by updating the row with
      # the smallest id when a VERIFY_ROW_EVENT is received
      batches_verified = 0
      @ghostferry.on_status(Ghostferry::Status::VERIFY_ROW_EVENT) do
        batches_verified += 1
        if batches_verified == 1
          updated_id = @dbs.source.query("SELECT id FROM #{DbManager::DEFAULT_FULL_TABLE_NAME} ORDER BY id ASC LIMIT 1").first["id"]
          @dbs.source.query("UPDATE #{DbManager::DEFAULT_FULL_TABLE_NAME} SET data = 'modified' WHERE id = #{updated_id}")
        elsif batches_verified >= 2
          @ghostferry.send_signal("TERM")
        end
      end

      dumped_state = @ghostferry.run_expecting_interrupt(environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
      assert_basic_fields_exist_in_dumped_state(dumped_state)
      assert_verifier_stage_exists_in_dumped_state(dumped_state)
      assert_equal [updated_id], dumped_state["VerifierStage"]["ReverifyStore"]["gftest.test_table_1"]
    end

    # We need to ensure the reverify store is actually reverified by the
    # resuming iterative verifier. So we delete the updated_id from the target
    # during the downtime. The verification should thus fail.

    @dbs.target.query("DELETE FROM #{DbManager::DEFAULT_FULL_TABLE_NAME} WHERE id = #{updated_id}")

    verified = false
    with_isolated_setup_and_teardown do
      @ghostferry.on_status(Ghostferry::Status::VERIFIED) do |num_failed, *incorrect_tables|
        verified = true

        assert_equal "1", num_failed
        assert_equal ["gftest.test_table_1"], incorrect_tables
      end

      @ghostferry.run(dumped_state, environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
    end

    assert verified
  end

  def test_iterative_verifier_reverify_rows_changed_after_reconciliation_but_before_verify_before_cutover
    @dbs.seed_simple_database_with_single_table

    dumped_state = nil
    with_isolated_setup_and_teardown do
      batches_verified = 0
      @ghostferry.on_status(Ghostferry::Status::VERIFY_ROW_EVENT) do
        batches_verified += 1
        if batches_verified >= 2
          @ghostferry.send_signal("TERM")
        end
      end

      dumped_state = @ghostferry.run_expecting_interrupt(environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
      assert_basic_fields_exist_in_dumped_state(dumped_state)
      assert_verifier_stage_exists_in_dumped_state(dumped_state)
    end

    verified = false
    with_isolated_setup_and_teardown do
      changed_id = 0
      @ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
        id = @dbs.source.query("SELECT id FROM #{DbManager::DEFAULT_FULL_TABLE_NAME} ORDER BY id ASC LIMIT 1").first["id"]
        @dbs.source.query("UPDATE #{DbManager::DEFAULT_FULL_TABLE_NAME} SET data = 'modified' WHERE id = #{id}")
        @dbs.target.query("DELETE FROM #{DbManager::DEFAULT_FULL_TABLE_NAME} WHERE id = #{id}")
      end

      @ghostferry.on_status(Ghostferry::Status::VERIFIED) do |num_failed, *incorrect_tables|
        verified = true

        assert_equal "1", num_failed
        assert_equal ["gftest.test_table_1"], incorrect_tables
      end

      @ghostferry.run(dumped_state, environment: {Ghostferry::ENV_KEY_ITERATIVE_VERIFIER => "1"})
    end

    assert verified
  end
end
