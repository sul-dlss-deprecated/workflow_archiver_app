require 'rails_helper'

describe CompletedWorkflow do
  subject { described_class.new }

  describe '#to_bind_hash' do
    it 'is a hash of the methods send into ArchiveCriteria, except version' do
      subject.version = '1'
      subject.repository = 'sdr'
      expect(subject.to_bind_hash).to eq({repository: 'sdr'})
    end
  end

  describe '#setup_from_query' do
    it 'sets attributes based on a hash argument' do
      expect(subject.repository).to be_nil
      expect(subject.druid).to be_nil
      expect(subject.datastream).to be_nil
      subject.setup_from_query(
        { repository: 'sdr', druid: 'abc123', datastream: 'workflow-something' }
      )
      expect(subject.repository).to eq 'sdr'
      expect(subject.druid).to eq 'abc123'
      expect(subject.datastream).to eq 'workflow-something'
    end

    it 'sets the current version from dor' do
      expect(subject.version).to be_nil
      expect(subject).to receive(:current_version_from_dor).and_return('2')
      subject.setup_from_query({})
      expect(subject.version).to eq '2'
    end

    it 'returns self' do
      expect(subject.setup_from_query({})).to be_a described_class
    end
  end
end
