package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.empathy.academy.search.exception.types.IndexDoesNotExistException;
import co.empathy.academy.search.exception.types.InternalServerException;
import co.empathy.academy.search.utils.ElasticUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.json.JsonValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@Tag(name = "Query controller", description = "Allows to perform queries on the IMDB database")
@RestController
@RequestMapping("/api")
public class QueryController {
    private final ElasticsearchClient client = ElasticUtils.getClient();

    /**
     * Returns the name of the indices currently stored on the elastic container
     * @return Json with the names
     */
    @Operation(summary = "Performs a query on the database")
    @Parameter(name = "q", description = "String with the query", required = true)
    @ApiResponse(responseCode = "200", description="Documents obtained", content = { @Content(mediaType= "application/json")})
    @GetMapping("/q")
    public List<JsonValue> query(@RequestParam String q) {
        argumentCheck(q, "query");

        try {
            var results = client.search(_0 -> _0
                    .query(_1 -> _1
                            .queryString(_2 -> _2
                                    .query(q)
                                    .defaultField("primaryTitle")
                            )
                    )
                    , JsonData.class
            );

        return getResults(results);
        } catch(IOException e) {
            throw new InternalServerException("There was a problem connecting to ElasticSearch", e);
        } catch (ElasticsearchException e) {
            throw new IndexDoesNotExistException("There was a problem processing your request", e);
        }
    }

    private void argumentCheck(String param, String name) {
        if(param == null) {
            throw new IllegalArgumentException("The parameter " + name + " must not be null");
        }
    }

    private List<JsonValue> getResults(SearchResponse<JsonData> response) {
        return response.hits().hits().stream().filter(x -> x.source() != null).map(x -> x.source().toJson()).toList();
    }
}
