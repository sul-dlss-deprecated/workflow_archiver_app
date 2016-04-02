class WorkflowSql
  WF_COLUMNS = %w(id druid datastream process status error_msg error_txt datetime attempts lifecycle elapsed repository note priority lane_id).freeze
  WORKFLOW_TABLE = 'workflow'.freeze
  WORKFLOW_ARCHIVE_TABLE = 'workflow_archive'.freeze

  def initialize(workflow)
    @repository = workflow.repository
    @version    = workflow.version
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

  def self.completed_sql
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

  private

  attr_reader :repository, :version

  def wf_column_string
    WF_COLUMNS.join(",\n")
  end

  def wf_archive_column_string
    WF_COLUMNS.map { |col| "#{WORKFLOW_TABLE}.#{col}" }.join(",\n")
  end
end
