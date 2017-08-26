package com.ryuusen.kafka.client;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

public abstract class ClientConfigBuilder implements IClientConfigBuilder{
	
	protected Properties props;
	
	public ClientConfigBuilder(){
		this(new Properties());
	}
	
	public ClientConfigBuilder(Properties props){
		this.props = props;
		setDefaults();
	}
	
	@Override
	public ClientConfigBuilder addMetricReporter(String reporterClass){
		String original = props.getProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG);
		props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, appendToCsv(original, reporterClass));
		return this;
	}
	
	@Override
	public ClientConfigBuilder withMetricReporterConfigs(String numSamples, String sampleWindowMs, String recordingLevel){
		//TODO
		return this;
	}
	
	@Override
	public ClientConfigBuilder withSecurityProtocol(String protocol){
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
		return this;
	}
	
	@Override
	public ClientConfigBuilder withSslKeystore(String keystoreLocation){
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
		return this;
	}
	
	@Override
	public ClientConfigBuilder withSslKeystore(String keystoreLocation, String keystorePassword, String keyPassword){
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
		props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
		props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
		return this;
	}
	
	@Override
	public ClientConfigBuilder withSslTruststore(String truststoreLocation){
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, truststoreLocation);
		return this;
	}
	
	@Override
	public ClientConfigBuilder withSslTruststore(String truststoreLocation, String truststorePassword){
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, truststoreLocation);
		props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, truststorePassword);
		return this;
	}
	
	public IClientConfigBuilder withDefaultTopic(String defaultTopic){
		props.put(IClientConfigBuilder.DEFAULT_TOPIC_CONFIG, defaultTopic);
		return this;
	}
	
	@Override
	public ClientConfigBuilder withProperty(String config, String value){
		props.put(config, value);
		return this;
	}
	
	protected String appendToCsv(String original, String add){
		if(null == add|| add.isEmpty()){
			throw new IllegalArgumentException("String to append cannot be null or empty");
		}
		if(null == original || original.isEmpty()){
			return add;
		}
		return original + "," + add;
	}

}
