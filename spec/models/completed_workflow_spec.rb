require 'rails_helper'

describe CompletedWorkflow do
  subject { described_class.new }

  describe '#to_bind_hash' do
    it 'is a hash of the methods send into ArchiveCriteria, except version' do
      subject.repository = 'sdr'
      expect(subject.to_bind_hash).to eq({repository: 'sdr'})
    end
  end

  describe '#version' do
    it 'fetches the current version from DOR' do
      expect(subject).to receive(:current_version_from_dor).and_return('6')
      expect(subject.version).to eq '6'
    end
  end

  describe 'class methods' do
    describe '#all' do
      it 'is an enumerator of rows' do
        expect(described_class.all).to be_a Enumerator
      end
    end
  end
end
