package com.ryuusen.kafka.client.consumer.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public abstract class AbstractConsumer<K, V> implements Consumer<K, V> {

	Consumer<K, V> primary;

	public AbstractConsumer(){
		
	}
	
	public AbstractConsumer(Map<String, Object> props){
		primary = new KafkaConsumer<K, V>(props);
	}
	
	public AbstractConsumer(Properties props){
		primary = new KafkaConsumer<K, V>(props);
	}
	
	public AbstractConsumer(Consumer<K, V> primary) {
		this.primary = primary;
	}
	
	protected Consumer<K, V> primary(){
		return primary;
	}
	
	protected void setPrimary(Consumer<K, V> primary){
		this.primary = primary;
	}

	@Override
	public Set<TopicPartition> assignment() {
		return primary.assignment();
	}

	@Override
	public Set<String> subscription() {
		return primary.subscription();
	}

	@Override
	public void subscribe(Collection<String> topics) {
		primary.subscribe(topics);
	}

	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
		primary.subscribe(topics, callback);
	}

	@Override
	public void assign(Collection<TopicPartition> partitions) {
		primary.assign(partitions);
	}

	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
		primary.subscribe(pattern, callback);
	}

	@Override
	public void unsubscribe() {
		primary.unsubscribe();
	}

	@Override
	public ConsumerRecords<K, V> poll(long timeout) {
		return primary.poll(timeout);
	}

	@Override
	public void commitSync() {
		primary.commitSync();
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
		primary.commitSync(offsets);
	}

	@Override
	public void commitAsync() {
		primary.commitAsync();
	}

	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		primary.commitAsync(callback);
	}

	@Override
	public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		primary.commitAsync(offsets, callback);
	}

	@Override
	public void seek(TopicPartition partition, long offset) {
		primary.seek(partition, offset);
	}

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		primary.seekToBeginning(partitions);
	}

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		primary.seekToEnd(partitions);
	}

	@Override
	public long position(TopicPartition partition) {
		return primary.position(partition);
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		return primary.committed(partition);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return primary.metrics();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return primary.partitionsFor(topic);
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		return primary.listTopics();
	}

	@Override
	public Set<TopicPartition> paused() {
		return primary.paused();
	}

	@Override
	public void pause(Collection<TopicPartition> partitions) {
		primary.pause(partitions);
	}

	@Override
	public void resume(Collection<TopicPartition> partitions) {
		primary.resume(partitions);
	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		return primary.offsetsForTimes(timestampsToSearch);
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
		return primary.beginningOffsets(partitions);
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		return primary.endOffsets(partitions);
	}

	@Override
	public void close() {
		primary.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		primary.close(timeout, unit);
	}

	@Override
	public void wakeup() {
		primary.wakeup();
	}
}
