require 'rails_helper'
require 'yaml'

describe WorkflowArchiver do
  subject do
    WorkflowArchiver.new(db_connection: $sequel_db)
  end

  before do
    workflows = CompletedWorkflow.connection.from(:workflow)

    data = YAML.load <<-EOF
---
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: cleanup
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: register
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: sdr-ingest-archive
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: register-object
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: etdSubmitWF
  :process: cleanup
  :status: waiting
  :repository: dor
- :druid: integration:345
  :datastream: etdSubmitWF
  :process: register
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: sdrIngestWF
  :process: register-sdr
  :status: completed
  :repository: sdr
- :druid: integration:678
  :datastream: googleScannedBookWF
  :process: cleanup
  :status: waiting
  :repository: dor
- :druid: integration:678
  :datastream: googleScannedBookWF
  :process: register
  :status: completed
  :repository: dor
- :druid: integration:568
  :datastream: sdrIngestWF
  :process: cleanup
  :status: completed
  :repository: sdr
- :druid: integration:999
  :datastream: etdSubmitWF
  :process: cleanup
  :status: completed
  :repository: sdr
- :druid: integration:678
  :datastream: sdrIngestWF
  :process: register-sdr
  :status: completed
  :repository:
    EOF

    data.each do |d|
      workflows.insert d
    end

  end

  after do
    CompletedWorkflow.connection.from(:workflow).where(Sequel.like(:druid, 'integration:%')).delete
    CompletedWorkflow.connection.from(:workflow_archive).where(Sequel.like(:druid, 'integration:%')).delete
  end

  describe '#archive_rows' do
    before do
      allow_any_instance_of(CompletedWorkflow).to receive(:current_version_from_dor).and_return(1)
    end

    it 'copies completed workflow rows to the archive table' do
      expect { subject.archive }.to change { CompletedWorkflow.connection.from(:workflow_archive).count }.from(0).to(8)
      expect(CompletedWorkflow.all.count).to eq 0

      archived = CompletedWorkflow.connection.from(:workflow_archive).to_a

      expect(archived).to include(hash_including(druid: 'integration:678', datastream: 'sdrIngestWF'))
      expect(archived).not_to include(hash_including(druid: 'integration:678', datastream: 'googleScannedBookWF'))
    end
  end
end
