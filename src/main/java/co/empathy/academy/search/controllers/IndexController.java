package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.search.utils.ElasticUtils;
import co.empathy.academy.search.utils.ReadDataUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
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

    @GetMapping("/index_documents")
    public void indexDocuments() {
        try {
            try {
                client.indices().delete(c -> c.index("films"));
            } catch(Exception e) {
                //in case the index does not exist
            }

            client.indices().create(c -> c.index("films"));

            putMapping();

            ReadDataUtils.getData("src/main/resources/title.basics.tsv");

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void putMapping() {
        try {
            client.indices().putMapping(_0 -> _0
                    .index("films")
                    .properties("titleType", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                            )
                    )
                    .properties("primaryTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                                    .fields("raw", _3 -> _3
                                            .keyword(_4 -> _4
                                                    .boost(1.0)
                                            )
                                    )
                            )

                    )
                    .properties("originalTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                            )
                    )
                    .properties("isAdult", _1 -> _1
                            .boolean_(_2 -> _2
                                    .boost(1.0)

                            )
                    )
                    .properties("startYear", _1 -> _1
                            .integer(_2 -> _2
                                    .nullValue(0)
                            )
                    )
                    .properties("endYear", _1 -> _1
                            .integer(_2 -> _2
                                    .nullValue(0)
                            )
                    )
                    .properties("runtimeMinutes", _1 -> _1
                            .integer(_2 -> _2
                                    .nullValue(0)
                            )
                    )
                    .properties("genres", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                            )
                    )
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
