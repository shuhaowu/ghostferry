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
    with_new_standard_components do
      use_datawriter
      interrupt_ghostferry_when_some_batches_are_copied

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

    with_new_standard_components do
      use_datawriter
      @ghostferry.run(dumped_state)

      assert_test_table_is_identical
    end
  end
end
