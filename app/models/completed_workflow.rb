class CompletedWorkflow
  attr_accessor :repository, :druid, :datastream

  delegate :to_copy_sql, :to_delete_sql, to: :workflow_sql

  def initialize(attributes = {})
    @repository = attributes[:repository]
    @druid      = attributes[:druid]
    @datastream = attributes[:datastream]
  end

  def archive!
    LyberCore::Log.info "Archiving #{self}"

    connection.transaction do
      connection.run Sequel::SQL::PlaceholderLiteralString.new(to_copy_sql, to_bind_hash)

      LyberCore::Log.debug '  Removing old workflow rows'

      connection.run Sequel::SQL::PlaceholderLiteralString.new(to_delete_sql, to_bind_hash)
    end
  end

  def version
    @version ||= current_version_from_dor
  end

  def to_bind_hash
    [:repository, :druid, :datastream].each_with_object({}) do |meth, hash|
      hash[meth] = send(meth) if send(meth)
    end
  end

  class << self
    attr_writer :connection

    def all
      return to_enum(:all) unless block_given?

      connection.fetch(WorkflowSql.completed_sql) do |row|
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
  end

  private

  def connection
    self.class.connection
  end

  def workflow_sql
    @workflow_sql ||= WorkflowSql.new(self)
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
