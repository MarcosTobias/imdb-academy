package co.empathy.academy.search.utils;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticHighClientUtils {
    //Credentials used for the connection
    private static final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    //Creation of the rest client with the corresponding credentials
    private static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200),
                    new HttpHost("elasticsearch", 9200))
                    .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                        credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials("elastic", "searchPathRules"));

                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    ));

    private ElasticHighClientUtils(){}

    /**
     * Returns the Elasticsearch client for connecting to it from anywhere
     * @return ElasticsearchClient
     */
    public static RestHighLevelClient getClient() {
        return client;
    }
}
