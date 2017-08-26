package com.ryuusen.kafka.client.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

public interface IConsumer<K, V> extends Consumer<K, V> {
	public void processConfigs(Properties props);
	public void commitSync(ConsumerRecord<K, V> record);
	public void commitAsync(ConsumerRecord<K, V> record);
	public void commitAsync(ConsumerRecord<K, V> record, OffsetCommitCallback callback);
}
