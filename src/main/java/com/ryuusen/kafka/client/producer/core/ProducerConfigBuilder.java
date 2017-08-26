package com.ryuusen.kafka.client.producer.core;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.ryuusen.kafka.client.ClientConfigBuilder;


public class ProducerConfigBuilder extends ClientConfigBuilder implements IProducerConfigBuilder{
	
	public ProducerConfigBuilder(){
		super();
	}
	
	public ProducerConfigBuilder(Properties props){
		super(props);
	}
	
	@Override
	public void setDefaults(){
		withBatching("16384", "50");
	}
	
	@Override
	public ProducerConfigBuilder connectTo(String bootstrapServers, String clientId){
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withSerializers(String keySerializer, String valueSerializer){
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withBatching(String batchSize, String lingerMs){
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
		props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withIdempotence(){
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withTransactions(String transactionalId){
		props.put(ProducerConfig.TRANSACTIONAL_ID_DOC, transactionalId);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withTransactions(String transactionalId, String transactionTimeoutMs){
		props.put(ProducerConfig.TRANSACTIONAL_ID_DOC, transactionalId);
		props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withCompression(String compressionType){
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withPartitionerClass(String partitionerClass){
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
		return this;
	}

	@Override
	public ProducerConfigBuilder addInterceptor(String interceptorClass){
		String original = props.getProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
		props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, super.appendToCsv(original, interceptorClass));
		return this;
	}

	@Override
	public ProducerConfigBuilder addMetricReporter(String reporterClass){
		super.addMetricReporter(reporterClass);
		return this;
	}

	@Override
	public ProducerConfigBuilder withMetricReporterConfigs(String numSamples, String sampleWindowMs, String recordingLevel){
		super.withMetricReporterConfigs(numSamples, sampleWindowMs, recordingLevel);
		return this;
	}

	@Override
	public ProducerConfigBuilder withSecurityProtocol(String protocol){
		super.withSecurityProtocol(protocol);
		return this;
	}

	@Override
	public ProducerConfigBuilder withSslKeystore(String keystoreLocation){
		super.withSslKeystore(keystoreLocation);
		return this;
	}

	@Override
	public ProducerConfigBuilder withSslKeystore(String keystoreLocation, String keystorePassword, String keyPassword){
		super.withSslKeystore(keystoreLocation, keystorePassword, keyPassword);
		return this;
	}

	@Override
	public ProducerConfigBuilder withSslTruststore(String truststoreLocation){
		super.withSslTruststore(truststoreLocation);
		return this;
	}

	@Override
	public ProducerConfigBuilder withSslTruststore(String truststoreLocation, String truststorePassword){
		super.withSslTruststore(truststoreLocation, truststorePassword);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withDefaultTopic(String defaultTopic){
		super.withDefaultTopic(defaultTopic);
		return this;
	}
	
	@Override
	public ProducerConfigBuilder withProperty(String config, String value){
		super.withProperty(config, value);
		return this;
	}
	
	@Override
	public Properties build(){
		return new Properties(props);
	}
	
}
