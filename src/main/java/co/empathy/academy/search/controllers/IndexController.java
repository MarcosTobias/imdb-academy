package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.search.utils.ElasticUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

@RestController
public class IndexController {
    private final ElasticsearchClient client = ElasticUtils.getClient();
    @GetMapping("/_cat/indices")
    public Map<String, IndexState> getIndices() {
        try {
            return client.indices().get(c -> c.index("*")).result();

        } catch(IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @PutMapping("/{index}")
    public boolean createIndex(@PathVariable String index) {
        try {
            return Boolean.TRUE.equals(client.indices().create(c -> c.index(index)).acknowledged());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    @DeleteMapping("/{index}")
    public boolean deleteIndex(@PathVariable String index) {
        try {
            return client.indices().delete(c -> c.index(index)).acknowledged();
        } catch(IOException e) {
            e.printStackTrace();
        }

        return false;
    }
}
