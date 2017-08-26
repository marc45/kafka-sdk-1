package com.ryuusen.kafka.client.consumer;

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

import com.ryuusen.kafka.client.IClientConfigBuilder;

public class CoreConsumer<K, V> implements IConsumer<K, V> {

	Consumer<K, V> baseConsumer;
	String defaultTopics;
	
	public CoreConsumer(Properties props){
		processConfigs(props);
		baseConsumer = new KafkaConsumer<>(props);
	}
	
	public CoreConsumer(Properties props, Consumer<K, V> baseConsumer){
		processConfigs(props);
		this.baseConsumer = baseConsumer;
	}
	
	@Override
	public void processConfigs(Properties props) {
		this.defaultTopics = props.getProperty(IClientConfigBuilder.DEFAULT_TOPIC_CONFIG);
	}
	
	@Override
	public void commitSync(ConsumerRecord<K, V> record) {
		this.commitSync(toOffsetMap(record));
	}

	@Override
	public void commitAsync(ConsumerRecord<K, V> record){
		this.commitAsync(record, null);
	}
	
	@Override
	public void commitAsync(ConsumerRecord<K, V> record, OffsetCommitCallback callback){
		this.commitAsync(toOffsetMap(record), callback);
	}

	private Map<TopicPartition, OffsetAndMetadata> toOffsetMap(ConsumerRecord<K, V> record) {
		return Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
				new OffsetAndMetadata(record.offset()));
	}

	@Override
	public Set<TopicPartition> assignment() {
		return baseConsumer.assignment();
	}

	@Override
	public Set<String> subscription() {
		return baseConsumer.subscription();
	}

	@Override
	public void subscribe(Collection<String> topics) {
		subscribe(topics, null);
	}

	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
		baseConsumer.subscribe(topics, callback);
	}

	@Override
	public void assign(Collection<TopicPartition> partitions) {
		baseConsumer.assign(partitions);
	}

	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
		baseConsumer.subscribe(pattern, callback);
	}

	@Override
	public void unsubscribe() {
		baseConsumer.unsubscribe();
	}

	@Override
	public ConsumerRecords<K, V> poll(long timeout) {
		return baseConsumer.poll(timeout);
	}

	@Override
	public void commitSync() {
		baseConsumer.commitSync();
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
		baseConsumer.commitSync(offsets);
	}

	@Override
	public void commitAsync() {
		baseConsumer.commitAsync();
	}

	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		baseConsumer.commitAsync(callback);
	}

	@Override
	public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		baseConsumer.commitAsync(offsets, callback);
	}

	@Override
	public void seek(TopicPartition partition, long offset) {
		baseConsumer.seek(partition, offset);
	}

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		baseConsumer.seekToBeginning(partitions);
	}

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		baseConsumer.seekToEnd(partitions);
	}

	@Override
	public long position(TopicPartition partition) {
		return baseConsumer.position(partition);
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		return baseConsumer.committed(partition);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return baseConsumer.metrics();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return baseConsumer.partitionsFor(topic);
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		return baseConsumer.listTopics();
	}

	@Override
	public Set<TopicPartition> paused() {
		return baseConsumer.paused();
	}

	@Override
	public void pause(Collection<TopicPartition> partitions) {
		baseConsumer.pause(partitions);
	}

	@Override
	public void resume(Collection<TopicPartition> partitions) {
		baseConsumer.resume(partitions);
	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		return baseConsumer.offsetsForTimes(timestampsToSearch);
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
		return baseConsumer.beginningOffsets(partitions);
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		return baseConsumer.endOffsets(partitions);
	}

	@Override
	public void close() {
		baseConsumer.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		baseConsumer.close(timeout, unit);
	}

	@Override
	public void wakeup() {
		baseConsumer.wakeup();
	}
}
