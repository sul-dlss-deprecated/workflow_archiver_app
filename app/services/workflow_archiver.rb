require 'faraday'
require 'confstruct'
require 'lyber_core'
require 'sequel'

class WorkflowArchiver
  WF_COLUMNS = %w(id druid datastream process status error_msg error_txt datetime attempts lifecycle elapsed repository note priority lane_id)

  # These attributes mostly used for testing
  attr_reader :errors

  def self.config
    @@conf ||= Confstruct::Configuration.new
  end

  # Sets up logging and connects to the database.  By default it reads values from constants:
  #  WORKFLOW_DB_LOGIN, WORKFLOW_DB_PASSWORD, WORKFLOW_DB_URI, DOR_SERVICE_URI but can be overriden with the opts Hash
  # @param [Hash] opts Options to override database parameters
  # @option opts [String] :db_uri ('WORKFLOW_DB_URI') Database uri
  # @option opts [String] :wf_table ('workflow') Name of the active workflow table
  # @option opts [String] :wfa_table ('workflow_archive') Name of the workflow archive table
  # @option opts [Integer] :retry_delay (5) Number of seconds to sleep between retries of database operations
  def initialize(opts = {})
    @conn = opts[:db_connection]
    @db_uri                 = opts.fetch(:db_uri, WorkflowArchiver.config.db_uri).freeze
    @workflow_table         = opts.include?(:wf_table)    ? opts[:wf_table]    : 'workflow'
    @workflow_archive_table = opts.include?(:wfa_table)   ? opts[:wfa_table]   : 'workflow_archive'
    @retry_delay            = opts.include?(:retry_delay) ? opts[:retry_delay] : 5
    # initialize some counters
    @errors = 0
    @archived = 0
  end

  def conn
    @conn ||= Sequel.connect(@db_uri)
  end

  # @return [String] The columns appended with comma and newline
  def wf_column_string
    WF_COLUMNS.join(",\n")
  end

  # @return [String] The columns prepended with 'w.' and appended with comma and newline
  def wf_archive_column_string
    WF_COLUMNS.map { |col| "#{@workflow_table}.#{col}" }.join(",\n")
  end

  # Use this as a one-shot method to archive all the steps of an object's particular datastream
  #   It will connect to the database, archive the rows, then logoff.  Assumes caller will set version (like the Dor REST service)
  # @note Caller of this method must handle destroying of the connection pool
  # @param [String] repository
  # @param [String] druid
  # @param [String] datastream
  # @param [String] version
  # def archive_one_datastream(repository, druid, datastream, version)
  #   criteria = [ArchiveCriteria.new(repository, druid, datastream, version)]
  #   archive_rows criteria
  # end

  # Copies rows from the workflow table to the workflow_archive table, then deletes the rows from workflow
  # Both operations must complete, or they get rolled back
  # @param [Array<ArchiveCriteria>] objs List of objects returned from {#find_completed_objects} and mapped to an array of ArchiveCriteria objects.
  def archive_rows(objs)
    objs.each do |obj|
      tries = 0
      begin
        tries += 1
        do_one_archive(obj)
        @archived += 1
      rescue => e
        LyberCore::Log.error "Rolling back transaction due to: #{e.inspect}\n" << e.backtrace.join("\n") << "\n!!!!!!!!!!!!!!!!!!"
        if tries < 3 # Retry this druid up to 3 times
          LyberCore::Log.error "  Retrying archive operation in #{@retry_delay} seconds..."
          sleep @retry_delay
          retry
        end
        LyberCore::Log.error "  Too many retries.  Giving up on #{obj.inspect}"

        @errors += 1
        if @errors >= 3
          LyberCore::Log.fatal('Too many errors. Archiving halted')
          break
        end
      end
    end # druids.each
  end

  # @param [ArchiveCriteria] workflow_info contains paramaters on the workflow rows to archive
  def do_one_archive(workflow_info)
    LyberCore::Log.info "Archiving #{workflow_info.inspect}"
    copy_sql = <<-EOSQL
      insert into #{@workflow_archive_table} (
        #{wf_column_string},
        version
      )
      select
        #{wf_archive_column_string},
        #{workflow_info.version} as version
      from #{@workflow_table}
      where #{@workflow_table}.druid =    :druid
      and #{@workflow_table}.datastream = :datastream
    EOSQL

    delete_sql = "delete from #{@workflow_table} where druid = :druid and datastream = :datastream "

    if(workflow_info.repository)
      copy_sql += "and #{@workflow_table}.repository = :repository"
      delete_sql += 'and repository = :repository'
    else
      copy_sql += "and #{@workflow_table}.repository IS NULL"
      delete_sql += 'and repository IS NULL'
    end

    conn.transaction do
      conn.run Sequel::SQL::PlaceholderLiteralString.new(copy_sql, workflow_info.to_bind_hash)

      LyberCore::Log.debug '  Removing old workflow rows'

      conn.run Sequel::SQL::PlaceholderLiteralString.new(delete_sql, workflow_info.to_bind_hash)
    end
  end

  # Does the work of finding completed objects and archiving the rows
  def archive
    objs = CompletedWorkflow.all

    if objs.none?
      LyberCore::Log.info 'Nothing to archive'
      exit true
    end
    LyberCore::Log.info "Found #{objs.size} completed workflows"
    archive_rows(objs)

    LyberCore::Log.info "DONE! Processed #{@archived.to_s} objects with #{@errors.to_s} errors" if @errors < 3
  end
end
