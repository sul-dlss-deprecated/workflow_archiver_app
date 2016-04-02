class WorkflowArchiver
  # These attributes mostly used for testing
  attr_reader :errors

  def self.config
    @@conf ||= Confstruct::Configuration.new
  end

  def initialize(opts = {})
    @retry_delay = opts.include?(:retry_delay) ? opts[:retry_delay] : 5
    @errors      = 0
    @archived    = 0
  end

  # Copies rows from the workflow table to the workflow_archive table, then deletes the rows from workflow
  # Both operations must complete, or they get rolled back
  # @param [Array<ArchiveCriteria>] objs List of objects returned from {#find_completed_objects} and mapped to an array of ArchiveCriteria objects.
  def archive_rows(objs)
    objs.each do |obj|
      tries = 0
      begin
        tries += 1
        obj.archive!
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

  # Does the work of finding completed objects and archiving the rows
  def archive
    objs = CompletedWorkflow.all

    if objs.none?
      LyberCore::Log.info 'Nothing to archive'
      exit true
    end
    LyberCore::Log.info "Found #{objs.size} completed workflows"
    archive_rows(objs)

    LyberCore::Log.info "DONE! Processed #{@archived} objects with #{@errors} errors" if @errors < 3
  end
end
