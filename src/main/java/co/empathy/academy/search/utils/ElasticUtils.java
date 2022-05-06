package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

/**
 * Handles creating the connection to the Elasticsearch container
 */

public class ElasticUtils {
    //Credentials used for the connection
    private static final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    //Creation of the rest client with the corresponding credentials
    private static final RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200), new HttpHost("elasticsearch", 9200)
    ).setHttpClientConfigCallback(httpAsyncClientBuilder -> {
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "searchPathRules"));

        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }).build();
    //Transport layer
    private static final ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    //Elasticsearch client, that is going to be used for the communication with the Elasticsearch container
    private static final ElasticsearchClient client = new ElasticsearchClient(transport);

    private ElasticUtils(){}

    /**
     * Returns the Elasticsearch client for connecting to it from anywhere
     * @return ElasticsearchClient
     */
    public static ElasticsearchClient getClient() {
        return client;
    }
}
