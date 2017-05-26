package com.ryuusen.kafka.client.consumer.core;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class CoreConsumer<K, V> extends AbstractConsumer<K, V> {

	public CoreConsumer(Properties props) {
		this.setPrimary(new KafkaConsumer<>(props));
	}

	public void commitSync(ConsumerRecord<K, V> record) {
		this.commitSync(toOffsetMap(record));
	}

	public void commitAsync(ConsumerRecord<K, V> record){
		this.commitAsync(record, null);
	}
	
	public void commitAsync(ConsumerRecord<K, V> record, OffsetCommitCallback callback){
		this.commitAsync(toOffsetMap(record), callback);
	}

	private Map<TopicPartition, OffsetAndMetadata> toOffsetMap(ConsumerRecord<K, V> record) {
		return Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
				new OffsetAndMetadata(record.offset()));
	}
}
