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
