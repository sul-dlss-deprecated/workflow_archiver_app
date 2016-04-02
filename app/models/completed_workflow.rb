class CompletedWorkflow
  attr_accessor :repository, :druid, :datastream, :version

  def setup_from_query(row_hash)
    self.repository = row_hash[:repository]
    self.druid      = row_hash[:druid]
    self.datastream = row_hash[:datastream]
    self.version    = current_version_from_dor
    self
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

      connection.fetch(completed_query) do |row|
        yield row
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

  def current_version_from_dor
    Faraday.get WorkflowArchiver.config.dor_service_uri + "/dor/v1/objects/#{druid}/versions/current"
  rescue Faraday::Error::ClientError => ise
    raise unless ise.inspect =~ /Unable to find.*in fedora/
    LyberCore::Log.warn ise.inspect.to_s
    LyberCore::Log.warn "Moving workflow rows with version set to '1'"
    '1'
  end
end
