package com.ryuusen.kafka.client.consumer.core;

import com.ryuusen.kafka.client.IClientConfigBuilder;

public interface IConsumerConfigBuilder extends IClientConfigBuilder {
	public IConsumerConfigBuilder withProperty(String config, String value);

	public IConsumerConfigBuilder connectTo(String bootstrapServers, String groupId, String clientId);

	public IConsumerConfigBuilder withDeserializers(String keyDeserializer, String valueDeserializer);

	public IConsumerConfigBuilder withAutoCommit();

	public IConsumerConfigBuilder withAutoCommit(String autoCommitIntervalMs);

	public IConsumerConfigBuilder withOffsetReset(String offsetResetStrategy);

	public IConsumerConfigBuilder withHeartbeats(String heartbeatTimeout, String pollIntervalTimeout);

	public IConsumerConfigBuilder withMaxPoll(String maxPollRecords);

	public IConsumerConfigBuilder withMaxPoll(String maxPollRecords, String maxPartitionFetchBytes, String maxFetchBytes);

	public IConsumerConfigBuilder addInterceptor(String interceptorClass);
	
	//Overriding from IClientConfigBuilder to ensure they return IConsumerConfigBuilder

	public IConsumerConfigBuilder addMetricReporter(String reporterClass);

	public IConsumerConfigBuilder withMetricReporterConfigs(String numSamples, String sampleWindowMs, String recordingLevel);

	public IConsumerConfigBuilder withSecurityProtocol(String protocol);

	public IConsumerConfigBuilder withSslKeystore(String keystoreLocation);

	public IConsumerConfigBuilder withSslKeystore(String keystoreLocation, String keystorePassword, String keyPassword);

	public IConsumerConfigBuilder withSslTruststore(String truststoreLocation);

	public IConsumerConfigBuilder withSslTruststore(String truststoreLocation, String truststorePassword);
	
	public IConsumerConfigBuilder withDefaultTopic(String defaultTopic);

}
