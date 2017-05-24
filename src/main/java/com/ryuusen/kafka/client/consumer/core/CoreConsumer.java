package com.ryuusen.kafka.client.consumer.core;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class CoreConsumer<K, V> implements Consumer<K, V> {

	Consumer<K, V> primaryConsumer;

	public CoreConsumer(Properties props) {
		primaryConsumer = new KafkaConsumer<>(props);
	}

	@Override
	public Set<TopicPartition> assignment() {
		return primaryConsumer.assignment();
	}

	@Override
	public Set<String> subscription() {
		return primaryConsumer.subscription();
	}

	@Override
	public void subscribe(Collection<String> topics) {
		primaryConsumer.subscribe(topics);
	}

	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
		primaryConsumer.subscribe(topics, callback);
	}

	@Override
	public void assign(Collection<TopicPartition> partitions) {
		primaryConsumer.assign(partitions);
	}

	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
		primaryConsumer.subscribe(pattern, callback);
	}

	@Override
	public void unsubscribe() {
		primaryConsumer.unsubscribe();
	}

	@Override
	public ConsumerRecords<K, V> poll(long timeout) {
		return primaryConsumer.poll(timeout);
	}

	@Override
	public void commitSync() {
		primaryConsumer.commitSync();
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
		primaryConsumer.commitSync(offsets);
	}

	public void commitSync(ConsumerRecord<K, V> record) {
		this.commitSync(toOffsetMap(record));
	}

	@Override
	public void commitAsync() {
		primaryConsumer.commitAsync();
	}

	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		primaryConsumer.commitAsync(callback);
	}

	@Override
	public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		primaryConsumer.commitAsync(offsets, callback);
	}

	public void commitAsync(ConsumerRecord<K, V> record){
		this.commitAsync(record, null);
	}
	
	public void commitAsync(ConsumerRecord<K, V> record, OffsetCommitCallback callback){
		this.commitAsync(toOffsetMap(record), callback);
	}

	@Override
	public void seek(TopicPartition partition, long offset) {
		primaryConsumer.seek(partition, offset);
	}

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		primaryConsumer.seekToBeginning(partitions);
	}

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		primaryConsumer.seekToEnd(partitions);
	}

	@Override
	public long position(TopicPartition partition) {
		return primaryConsumer.position(partition);
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		return primaryConsumer.committed(partition);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return primaryConsumer.metrics();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return primaryConsumer.partitionsFor(topic);
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		return primaryConsumer.listTopics();
	}

	@Override
	public Set<TopicPartition> paused() {
		return primaryConsumer.paused();
	}

	@Override
	public void pause(Collection<TopicPartition> partitions) {
		primaryConsumer.pause(partitions);
	}

	@Override
	public void resume(Collection<TopicPartition> partitions) {
		primaryConsumer.resume(partitions);
	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		return primaryConsumer.offsetsForTimes(timestampsToSearch);
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
		return primaryConsumer.beginningOffsets(partitions);
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		return primaryConsumer.endOffsets(partitions);
	}

	@Override
	public void close() {
		primaryConsumer.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		primaryConsumer.close(timeout, unit);
	}

	@Override
	public void wakeup() {
		primaryConsumer.wakeup();
	}

	private Map<TopicPartition, OffsetAndMetadata> toOffsetMap(ConsumerRecord<K, V> record) {
		return Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
				new OffsetAndMetadata(record.offset()));
	}
}
