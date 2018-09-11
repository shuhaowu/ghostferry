require "logger"
require "open3"
require "socket"
require "thread"
require "tmpdir"

module GhostferryIntegration
  class Ghostferry
    # Manages compiling, running, and communicating with Ghostferry.
    #
    # To use this class:
    #
    # ghostferry = Ghostferry.new("path/to/main.go")
    # ghostferry.on_status(Ghostferry::Status::BEFORE_ROW_COPY) do
    #   # do custom work here, such as injecting data into the database
    # end
    # ghostferry.run

    ENV_KEY_SOCKET_PATH = "GHOSTFERRY_INTEGRATION_SOCKET_PATH"

    SOCKET_PATH = ENV[ENV_KEY_SOCKET_PATH] || "/tmp/ghostferry-integration.sock"
    MAX_MESSAGE_SIZE = 256

    CONTINUE = "CONTINUE"

    module Status
      # This should be in sync with integrationferry.go
      READY = "READY"
      BINLOG_STREAMING_STARTED = "BINLOG_STREAMING_STARTED"
      ROW_COPY_COMPLETED = "ROW_COPY_COMPLETED"
      DONE = "DONE"

      BEFORE_ROW_COPY = "BEFORE_ROW_COPY"
      AFTER_ROW_COPY = "AFTER_ROW_COPY"
      BEFORE_BINLOG_APPLY = "BEFORE_BINLOG_APPLY"
      AFTER_BINLOG_APPLY = "AFTER_BINLOG_APPLY"
    end

    attr_reader :stdout, :stderr, :subprocess_exit_status

    def initialize(main_path, logger: nil, message_timeout: 30)
      @main_path = main_path
      @message_timeout = message_timeout
      @logger = logger
      if @logger.nil?
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::DEBUG
      end

      @tempdir = Dir.mktmpdir("ghostferry-integration")

      # full name relative to the ghostferry root dir, with / replaced with _
      # and the extension stripped.
      full_path = File.absolute_path(@main_path)
      full_path = full_path.split("/ghostferry/")[-1] # Assuming that ghostferry will show up in the path as its own directory
      binary_name = File.join(File.dirname(full_path), File.basename(full_path, ".*")).gsub("/", "_")
      @compiled_binary_path = File.join(@tempdir, binary_name)

      reset_state
    end

    def reset_state
      @status_handlers = {}
      @stop_requested = false

      @server = nil
      @server_started_notifier = Queue.new

      @subprocess_pid = 0
      @subprocess_exit_status = nil
      @subprocess_stdout = []
      @subprocess_stderr = []
    end

    def on_status(status, &block)
      raise "must specify a block" unless block_given?
      @status_handlers[status] = block
    end

    def compile_binary
      return if File.exists?(@compiled_binary_path)

      @logger.info("compiling test binary to #{@compiled_binary_path}")
      rc = system(
        "go", "build",
        "-o", @compiled_binary_path,
        @main_path
      )

      raise "could not compile ghostferry" unless rc
    end

    def start_server
      @server_thread = Thread.new do
        @server = UNIXServer.new(SOCKET_PATH)
        @server_started_notifier.push(true)

        reads = [@server]
        last_message_time = Time.now

        while (!@stop_requested && @subprocess_exit_status.nil?) do
          ready = IO.select(reads, nil, nil, 0.2)

          if ready.nil?
            next if Time.now - last_message_time < @message_timeout

            raise "ghostferry did not report to the integration test server for the last #{@message_timeout}"
          end

          last_message_time = Time.now

          # Each client should send one message, expects a message back, and
          # then close the connection.
          #
          # This is done because there are many goroutines operating in
          # parallel and sending messages over a shared connection would result
          # in multiplexing issues. Using separate connections gets around this
          # problem.
          ready[0].each do |socket|
            if socket == @server
              # A new message is to be sent by a goroutine
              client = @server.accept_nonblock
              reads << client
            elsif socket.eof?
              # A message was complete
              @logger.warn("client disconnected?")
              socket.close
              reads.delete(socket)
            else
              # Receiving a message
              status = socket.read_nonblock(MAX_MESSAGE_SIZE)
              @logger.debug("server received status: #{status}")

              @status_handlers[status].call unless @status_handlers[status].nil?
              socket.write(CONTINUE)

              reads.delete(socket)
            end
          end
        end

        @server.close
      end
    end

    def start_ghostferry
      @subprocess_thread = Thread.new do
        environment = {
          ENV_KEY_SOCKET_PATH => SOCKET_PATH
        }

        @logger.info("starting ghostferry test binary #{@compiled_binary_path}")
        Open3.popen3(environment, @compiled_binary_path) do |_, stdout, stderr, wait_thr|
          @subprocess_pid = wait_thr.pid

          reads = [stdout, stderr]
          until reads.empty? do
            ready_reads, _, _ = IO.select(reads)
            ready_reads.each do |reader|
              line = reader.gets
              if line.nil?
                # EOF effectively
                reads.delete(reader)
                next
              end

              if reader == stdout
                @subprocess_stdout << line
                @logger.debug("stdout: #{line}")
              elsif reader == stderr
                @subprocess_stderr << line
                @logger.debug("stderr: #{line}")
              end
            end
          end

          @subprocess_exit_status = wait_thr.value
          if @subprocess_exit_status.exitstatus != 0
            raise "ghostferry test binary returned non-zero status: #{@subprocess_exit_status}"
          end
          @subprocess_pid = 0
        end

        @logger.info("ghostferry test binary exitted: #{@subprocess_exit_status}")
      end
    end

    def wait_until_server_has_started
      @server_started_notifier.pop
      @logger.info("integration test server started and listening for connection")
    end

    def wait_until_ghostferry_run_is_complete
      @subprocess_thread.join
      @server_thread.join
    end

    def remove_socket
      File.unlink(SOCKET_PATH) if File.exists?(SOCKET_PATH)
    end

    def remove_binary
      FileUtils.remove_entry(@tempdir) unless @tempdir.nil?
    end

    def stop
      @stop_requested = true
      Process.kill("KILL", @subprocess_pid) if @subprocess_pid
      wait_until_ghostferry_run_is_complete
    end

    def run
      compile_binary
      start_server
      wait_until_server_has_started
      start_ghostferry
      wait_until_ghostferry_run_is_complete
    ensure
      remove_socket
      reset_state
    end
  end
end
