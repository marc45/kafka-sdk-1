package com.ryuusen.kafka.client.producer.core;

import com.ryuusen.kafka.client.IClientConfigBuilder;

public interface IProducerConfigBuilder extends IClientConfigBuilder{
	
	public IProducerConfigBuilder connectTo(String bootstrapServers, String clientId);
	
	public IProducerConfigBuilder withSerializers(String keySerializer, String valueSerializer);
	
	public IProducerConfigBuilder withBatching(String batchSize, String lingerMs);
	
	public IProducerConfigBuilder withIdempotence();
	
	public IProducerConfigBuilder withTransactions(String transactionalId);
	
	public IProducerConfigBuilder withTransactions(String transactionalId, String transactionTimeoutMs);
	
	public IProducerConfigBuilder withCompression(String compressionType);
	
	public IProducerConfigBuilder withPartitionerClass(String partitionerClass);
	
	public IProducerConfigBuilder addInterceptor(String interceptorClass);
	
	//Overriding from IClientConfigBuilder to ensure they return IProducerConfigBuilder

	public IProducerConfigBuilder addMetricReporter(String reporterClass);

	public IProducerConfigBuilder withMetricReporterConfigs(String numSamples, String sampleWindowMs, String recordingLevel);

	public IProducerConfigBuilder withSecurityProtocol(String protocol);

	public IProducerConfigBuilder withSslKeystore(String keystoreLocation);

	public IProducerConfigBuilder withSslKeystore(String keystoreLocation, String keystorePassword, String keyPassword);

	public IProducerConfigBuilder withSslTruststore(String truststoreLocation);

	public IProducerConfigBuilder withSslTruststore(String truststoreLocation, String truststorePassword);
	
	public IProducerConfigBuilder withDefaultTopic(String defaultTopic);
}
