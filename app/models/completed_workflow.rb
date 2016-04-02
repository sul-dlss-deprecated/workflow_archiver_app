class CompletedWorkflow
  WF_COLUMNS = %w(id druid datastream process status error_msg error_txt datetime attempts lifecycle elapsed repository note priority lane_id).freeze
  WORKFLOW_TABLE = 'workflow'.freeze
  WORKFLOW_ARCHIVE_TABLE = 'workflow_archive'.freeze

  attr_accessor :repository, :druid, :datastream

  def initialize(attributes = {})
    @repository = attributes[:repository]
    @druid      = attributes[:druid]
    @datastream = attributes[:datastream]
  end

  def version
    @version ||= current_version_from_dor
  end

  def to_bind_hash
    [:repository, :druid, :datastream].each_with_object({}) do |meth, hash|
      hash[meth] = send(meth) if send(meth)
    end
  end

  def to_copy_sql
    copy_sql = <<-EOSQL
      insert into #{WORKFLOW_ARCHIVE_TABLE} (
        #{wf_column_string},
        version
      )
      select
        #{wf_archive_column_string},
        #{version} as version
      from #{WORKFLOW_TABLE}
      where #{WORKFLOW_TABLE}.druid =    :druid
      and #{WORKFLOW_TABLE}.datastream = :datastream
    EOSQL

    copy_sql << if repository
                  "and #{WORKFLOW_TABLE}.repository = :repository"
                else
                  "and #{WORKFLOW_TABLE}.repository IS NULL"
                end
    copy_sql
  end

  def to_delete_sql
    delete_sql = "delete from #{WORKFLOW_TABLE} where druid = :druid and datastream = :datastream "
    delete_sql << if repository
                    'and repository = :repository'
                  else
                    'and repository IS NULL'
                  end
    delete_sql
  end

  class << self
    attr_writer :connection

    def all
      return to_enum(:all) unless block_given?

      connection.fetch(completed_query) do |row|
        yield new(row)
      end
    end

    def connection
      @connection ||= default_connection
    end

    private

    # TODO: Change db_uri configuration from WorkflowArchiver to rails-config
    def default_connection
      Sequel.connect(WorkflowArchiver.config.db_uri)
    end

    # TODO: Move all SQL somewhere else
    def completed_query
      <<-EOSQL
       select distinct repository, datastream, druid
       from workflow w1
       where w1.status in ('completed', 'skipped')
       and not exists
       (
          select *
          from workflow w2
          where w1.repository = w2.repository
          and w1.datastream = w2.datastream
          and w1.druid = w2.druid
          and w2.status not in ('completed', 'skipped')
       )
      EOSQL
    end
  end

  private

  def wf_column_string
    WF_COLUMNS.join(",\n")
  end

  def wf_archive_column_string
    WF_COLUMNS.map { |col| "#{WORKFLOW_TABLE}.#{col}" }.join(",\n")
  end

  # TODO: Change db_uri configuration from WorkflowArchiver to rails-config
  def current_version_from_dor
    Faraday.get WorkflowArchiver.config.dor_service_uri + "/dor/v1/objects/#{druid}/versions/current"
  rescue Faraday::Error::ClientError => ise
    raise unless ise.inspect =~ /Unable to find.*in fedora/
    LyberCore::Log.warn ise.inspect.to_s
    LyberCore::Log.warn "Moving workflow rows with version set to '1'"
    '1'
  end
end
