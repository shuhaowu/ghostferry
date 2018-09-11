require "logger"

require "mysql2"

module GhostferryIntegration
  class DbManager
    KNOWN_PORTS = [29291, 29292]

    DEFAULT_DB = "gftest"
    DEFAULT_TABLE = "test_table_1"

    def self.full_table_name(db, table)
      "`#{db}`.`#{table}`"
    end

    DEFAULT_FULL_TABLE_NAME = full_table_name(DEFAULT_DB, DEFAULT_TABLE)

    def self.default_db_config(port:)
      {
        host:      "127.0.0.1",
        port:      port,
        username:  "root",
        password:  "",
        encoding:  "utf8mb4",
        collation: "utf8mb4_unicode_ci",
      }
    end

    def self.transaction(connection)
      raise ArgumentError, "must pass a block" if !block_given?

      begin
        connection.query("BEGIN")
        yield
      rescue
        connection.query("ROLLBACK")
        raise
      else
        connection.query("COMMIT")
      end
    end

    def initialize(ports: KNOWN_PORTS, logger: nil)
      @ports = ports

      @connections = []
      ports.each do |port|
        @connections << Mysql2::Client.new(self.class.default_db_config(port: port))
      end

      @logger = logger
      if @logger.nil?
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::DEBUG
      end
    end

    # Do not use these methods in a separate thread as these are the raw mysql2
    # connections, which are not threadsafe.
    #
    # If you need to use connections in another thread for some reason, create
    # your own connections via source_db_config, such as in the case of the
    # DataWriter.
    def source
      @connections[0]
    end

    def target
      @connections[1]
    end

    def source_db_config
      self.class.default_db_config(port: @ports[0])
    end

    def target_db_config
      self.class.default_db_config(port: @ports[1])
    end

    def seed_random_data(connection, database_name: DEFAULT_DB, table_name: DEFAULT_TABLE, number_of_rows: 1111)
      full_table_name = self.class.full_table_name(database_name, table_name)

      connection.query("CREATE DATABASE IF NOT EXISTS #{database_name}")
      connection.query("CREATE TABLE IF NOT EXISTS #{full_table_name} (id bigint(20) not null auto_increment, data TEXT, primary key(id))")

      self.class.transaction(connection) do
        insert_statement = connection.prepare("INSERT INTO #{full_table_name} (id, data) VALUES (?, ?)")

        number_of_rows.times do
          insert_statement.execute(nil, GhostferryIntegration.rand_data)
        end
      end
    end

    def seed_simple_database_with_single_table
      # Setup the source database with data.
      max_id = 1111
      seed_random_data(source, number_of_rows: max_id)

      # Create some holes in the data.
      delete_statement = source.prepare("DELETE FROM #{self.class.full_table_name(DEFAULT_DB, DEFAULT_TABLE)} WHERE id = ?")
      140.times do
        delete_statement.execute(Random.rand(max_id) + 1)
      end

      # Setup the target database with no data but the correct schema.
      seed_random_data(target, number_of_rows: 0)
    end

    def reset_data
      @connections.each do |connection|
        connection.query("DROP DATABASE IF EXISTS `#{DEFAULT_DB}`")
      end
    end

    def source_and_target_table_metrics(tables: [DEFAULT_FULL_TABLE_NAME])
      source_metrics = {}
      target_metrics = {}

      tables.each do |table|
        source_metrics[table] = table_metric(source, table)
        target_metrics[table] = table_metric(target, table, sample_id: source_metrics[table][:sample_row]["id"])
      end

      [source_metrics, target_metrics]
    end

    def table_metric(conn, table, sample_id: nil)
      metrics = {}
      result = conn.query("CHECKSUM TABLE #{table}")
      metrics[:checksum] = result.first["Checksum"]

      result = conn.query("SELECT COUNT(*) AS cnt FROM #{table}")
      metrics[:row_count] = result.first["cnt"]

      if sample_id.nil?
        result = conn.query("SELECT * FROM #{table} ORDER BY RAND() LIMIT 1")
        metrics[:sample_row] = result.first
      else
        result = conn.query("SELECT * FROM #{table} WHERE id = #{sample_id} LIMIT 1")
        metrics[:sample_row] = result.first
      end

      metrics
    end
  end
end
