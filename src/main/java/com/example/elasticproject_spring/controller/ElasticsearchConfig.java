package com.example.elasticproject_spring.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author petenover@sina.com
 * @date 2021/9/22 17:27
 */
@Configuration
@Slf4j
public class ElasticsearchConfig {
	@Value("#{'${es.hosts}'.split(',')}")
	private List<String> hosts;
	@Value("${es.username}")
	private String username;
	@Value("${es.password}")
	private String password;
	@Value("${ca.fingerprint}")
	private String caFingerprint;
	@Value("${crt.name}")
	private String crtName;
	@Value("${es.https}")
	private Boolean httpsEnable;

	private static int connectTimeOut = 300_000; // 设置连接超时时间
	private static int socketTimeOut = 300_000; // 获取数据的超时时间
	private static int connectionRequestTimeOut = 180_000; // 从connect Manager获取Connection 超时时间

	private static final String SCHEME_HTTP = "http";
	private static final String SCHEME_HTTPS = "https";
	private static final String REGEX_COLON = ":";
	/**
	 * elasticsearch7.x restHighLevelClient for HTTP
	 * @return
	 */
	/*@Bean
	public RestHighLevelClient client() {
		return new RestHighLevelClient(restClientBuilder(false));
	}*/

	/**
	 * elasticsearch7.x restHighLevelClient for SSL(HTTPS)
	 * @return
	 * @throws IOException
	 */
	@Bean@Primary
	public RestHighLevelClient restHighLevelClient() {
		if(!Objects.isNull(httpsEnable) && httpsEnable){
			return new RestHighLevelClientBuilder(this.restClient()).setApiCompatibilityMode(true).build();
		}else{
			return new RestHighLevelClient(restClientBuilder(false));
		}
	}

	@Bean
	public ElasticsearchClient elasticsearchClient() {
		return new ElasticsearchClient(new RestClientTransport(this.restClient(), new JacksonJsonpMapper()));
	}

	private RestClient restClient() {
		return this.restClientBuilder(true).build();
	}

	private RestClientBuilder restClientBuilder(Boolean sslEnable) {
		System.out.println(this.hosts);
		HttpHost[] esHosts = hosts.stream().filter(StringUtils::isNotBlank).map(it -> {
			String[] split = it.split(REGEX_COLON);
			return new HttpHost(split[0], Integer.parseInt(split[1]), sslEnable ? SCHEME_HTTPS : SCHEME_HTTP);
		}).toArray(HttpHost[]::new);

		return RestClient.builder(esHosts).setRequestConfigCallback(requestConfigBuilder -> {
			requestConfigBuilder.setConnectTimeout(connectTimeOut);
			requestConfigBuilder.setSocketTimeout(socketTimeOut);
			requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
			return requestConfigBuilder;
		}).setHttpClientConfigCallback(httpClientBuilder -> {
			httpClientBuilder.disableAuthCaching().setKeepAliveStrategy((response, context) -> Duration.ofMinutes(5).toMillis());
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username, password));
			if (sslEnable){
				httpClientBuilder.setSSLContext(sslContextBuilder());
			}
			return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		});
	}

	/**
	 * 加载crt证书,设置ssl
	 * @return
	 */
	private SSLContext sslContextBuilder() {
		try {
			if (StringUtils.isNotBlank(caFingerprint)){
				log.info("=================开启指纹认证=================\n");
				log.info("{}",caFingerprint);
				return TransportUtils.sslContextFromCaFingerprint(caFingerprint);
			}
			ClassPathResource classPathResource = new ClassPathResource(StringUtils.isBlank(crtName) ? "http_ca.crt" : crtName);
			log.info("===================开启证书认证====================: {}",crtName);
			return TransportUtils.sslContextFromHttpCaCrt(classPathResource.getInputStream());
		}catch (Exception e){
			log.error("SSL CONTEXT ERROR: {}",e);
			throw new RuntimeException("SSL CONTEXT ERROR: "+e);
		}
	}
}
