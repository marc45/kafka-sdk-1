package com.ryuusen.kafka.client.consumer;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import com.ryuusen.kafka.client.ClientConfigBuilder;

public class ConsumerConfigBuilder extends ClientConfigBuilder implements IConsumerConfigBuilder{
	
	public ConsumerConfigBuilder(){
		super();
	}
	
	public ConsumerConfigBuilder(Properties props){
		super(props);
		setDefaults();
	}
	
	@Override
	public void setDefaults(){
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	}
	
	@Override
	public ConsumerConfigBuilder connectTo(String bootstrapServers, String groupId, String clientId){
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withDeserializers(String keyDeserializer, String valueDeserializer){
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withAutoCommit(){
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withAutoCommit(String autoCommitIntervalMs){
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withOffsetReset(String offsetResetStrategy){
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withHeartbeats(String heartbeatTimeout, String pollIntervalTimeout){
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, heartbeatTimeout);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollIntervalTimeout);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withMaxPoll(String maxPollRecords){
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withMaxPoll(String maxPollRecords, String maxPartitionFetchBytes, String maxFetchBytes){
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxFetchBytes);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder addInterceptor(String interceptorClass){
		String original = props.getProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
		props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, super.appendToCsv(original, interceptorClass));
		return this;
	}

	@Override
	public ConsumerConfigBuilder addMetricReporter(String reporterClass){
		super.addMetricReporter(reporterClass);
		return this;
	}

	@Override
	public ConsumerConfigBuilder withMetricReporterConfigs(String numSamples, String sampleWindowMs, String recordingLevel){
		super.withMetricReporterConfigs(numSamples, sampleWindowMs, recordingLevel);
		return this;
	}

	@Override
	public ConsumerConfigBuilder withSecurityProtocol(String protocol){
		super.withSecurityProtocol(protocol);
		return this;
	}

	@Override
	public ConsumerConfigBuilder withSslKeystore(String keystoreLocation){
		super.withSslKeystore(keystoreLocation);
		return this;
	}

	@Override
	public ConsumerConfigBuilder withSslKeystore(String keystoreLocation, String keystorePassword, String keyPassword){
		super.withSslKeystore(keystoreLocation, keystorePassword, keyPassword);
		return this;
	}

	@Override
	public ConsumerConfigBuilder withSslTruststore(String truststoreLocation){
		super.withSslTruststore(truststoreLocation);
		return this;
	}

	@Override
	public ConsumerConfigBuilder withSslTruststore(String truststoreLocation, String truststorePassword){
		super.withSslTruststore(truststoreLocation, truststorePassword);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withDefaultTopic(String defaultTopic){
		super.withDefaultTopic(defaultTopic);
		return this;
	}
	
	@Override
	public ConsumerConfigBuilder withProperty(String config, String value){
		super.withProperty(config, value);
		return this;
	}
	
	
	@Override
	public Properties build(){
		return new Properties(props);
	}

	
}
