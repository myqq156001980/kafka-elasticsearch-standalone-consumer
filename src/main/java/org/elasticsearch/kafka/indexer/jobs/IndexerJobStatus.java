package org.elasticsearch.kafka.indexer.jobs;

import org.elasticsearch.kafka.indexer.jmx.IndexerJobStatusMBean;

public class IndexerJobStatus implements IndexerJobStatusMBean{

	private long lastCommittedOffset;
	private IndexerJobStatusEnum jobStatus;
	private String topic;

	public IndexerJobStatus(long lastCommittedOffset,
			IndexerJobStatusEnum jobStatus, String topic) {
		super();
		this.lastCommittedOffset = lastCommittedOffset;
		this.jobStatus = jobStatus;
		this.topic = topic;
	}

	public long getLastCommittedOffset() {
		return lastCommittedOffset;
	}

	public void setLastCommittedOffset(long lastCommittedOffset) {
		this.lastCommittedOffset = lastCommittedOffset;
	}

	public IndexerJobStatusEnum getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(IndexerJobStatusEnum jobStatus) {
		this.jobStatus = jobStatus;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String toString() {
		StringBuilder sb  = new StringBuilder();
		sb.append("[IndexerJobStatus: {");
		sb.append("lastCommittedOffset=" + lastCommittedOffset);
		sb.append("currentTopic is=" + getTopic());
		sb.append("jobStatus=" + jobStatus.name());
		sb.append("}]");
		return sb.toString();
	}
	
}
