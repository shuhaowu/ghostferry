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
end
