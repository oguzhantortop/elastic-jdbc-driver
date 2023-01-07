package io.github.oguzhantortop.elasticjdbc;

import java.nio.charset.Charset;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.web.client.RestTemplate;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * "7wqP26.QyMZpexDIx"
 * @author oguzhan
 *
 */
public class ElasticUtil {
	public static String queryElasticSearch(String query) {
		RestTemplate restTemplate = new RestTemplate();
		if(ConfigStore.getInstance().isAuthenticated()) {
			restTemplate.getInterceptors().add(new BasicAuthenticationInterceptor(ConfigStore.getInstance().getProps().get("user").toString(), ConfigStore.getInstance().getProps().get("password").toString(), null));
		}

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		JsonObject queryObject = new JsonObject();
		queryObject.addProperty("query", query);
		HttpEntity<String> requestEntity = new HttpEntity(queryObject.toString().getBytes(Charset.defaultCharset()),
				headers);
		String elasticResourceUrl = ConfigStore.getInstance().getHost()+":"+ConfigStore.getInstance().getPort()+"/_sql?format=json";
		String sqlResponse = restTemplate.postForObject(elasticResourceUrl, requestEntity, String.class);
		return sqlResponse;
	}
	
	
	public static String scrollElastic(String scrollId) {
		RestTemplate restTemplate = new RestTemplate();
		if(ConfigStore.getInstance().isAuthenticated()) {
			restTemplate.getInterceptors().add(new BasicAuthenticationInterceptor(ConfigStore.getInstance().getProps().get("user").toString(), ConfigStore.getInstance().getProps().get("password").toString(), null));
		}

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		JsonObject queryObject = new JsonObject();
		queryObject.addProperty("cursor", scrollId);
		HttpEntity<String> requestEntity = new HttpEntity(queryObject.toString().getBytes(Charset.defaultCharset()),
				headers);
		String elasticResourceUrl = ConfigStore.getInstance().getHost()+":"+ConfigStore.getInstance().getPort()+"/_sql?format=json";
		String sqlResponse = restTemplate.postForObject(elasticResourceUrl, requestEntity, String.class);
		return sqlResponse;
	}
	
	public static String getElasticClusterName() {
		RestTemplate restTemplate = new RestTemplate();
		if(ConfigStore.getInstance().isAuthenticated()) {
			restTemplate.getInterceptors().add(new BasicAuthenticationInterceptor(ConfigStore.getInstance().getProps().get("user").toString(), ConfigStore.getInstance().getProps().get("password").toString(), null));
		}

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		String elasticResourceUrl = ConfigStore.getInstance().getHost()+":"+ConfigStore.getInstance().getPort();
		String sqlResponse = restTemplate.getForObject(elasticResourceUrl, String.class);
		Gson gson = new Gson();
		// This converts your String into whatever Type you want. In this case, we are
		// using JsonObjects from GSON.
		JsonObject json = gson.fromJson(sqlResponse, JsonObject.class);
		return json.get("cluster_name").getAsString();
	}
	
}
