package com.ryuusen.kafka.client;

import java.util.Properties;

public interface IClientConfigBuilder {
	
	public static final String DEFAULT_TOPIC_CONFIG = "default.topic";
	
	public void setDefaults();

	public IClientConfigBuilder addMetricReporter(String reporterClass);

	public IClientConfigBuilder withMetricReporterConfigs(String numSamples, String sampleWindowMs, String recordingLevel);

	public IClientConfigBuilder withSecurityProtocol(String protocol);

	public IClientConfigBuilder withSslKeystore(String keystoreLocation);

	public IClientConfigBuilder withSslKeystore(String keystoreLocation, String keystorePassword, String keyPassword);

	public IClientConfigBuilder withSslTruststore(String truststoreLocation);

	public IClientConfigBuilder withSslTruststore(String truststoreLocation, String truststorePassword);
	
	public IClientConfigBuilder withDefaultTopic(String defaultTopic);
	
	public IClientConfigBuilder withProperty(String config, String value);
	
	public Properties build();
}
