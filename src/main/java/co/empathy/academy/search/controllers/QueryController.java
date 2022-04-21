package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchRequest;
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
import jakarta.json.Json;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

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
    @GetMapping("/search")
    public String search(
            @RequestParam(required = false) Optional<String> q,
            @RequestParam(required = false) Optional<List<String>> filter,
            @RequestParam(required = false) Optional<String> agg) {

        var request = new SearchRequest.Builder().index("films");

        if(q.isPresent()) request = addSearch(q.get(), request);

        if(filter.isPresent())
            for(String s : filter.get())
                request = addFilter(s, request);

        if(agg.isPresent()) request = addAgg(agg.get(), request);

        var response = runSearch(request.build());

        return agg.isPresent() ? getAggs(response, agg.get() + "_agg") : getHits(response);
    }

    private SearchRequest.Builder addSearch(String q, SearchRequest.Builder request) {
        return request
                .query(_1 -> _1
                        .queryString(_2 -> _2
                                .query(q)
                                .defaultField("primaryTitle")
                        )
                );
    }

    private SearchRequest.Builder addFilter(String filter, SearchRequest.Builder request) {
        return request
                .query(_0 -> _0
                        .bool(_1 -> _1
                                .filter(_2 -> _2
                                        .match(_3 -> _3
                                                .field("genre")
                                                .query(filter)
                                        )
                                )
                        )
                );
    }

    private SearchRequest.Builder addAgg(String agg, SearchRequest.Builder request) {
        return request
                .size(0)
                .aggregations(agg + "_agg", _0 -> _0
                        .terms(_1 -> _1
                                .field(agg)
                        )
                );
    }

    private SearchResponse<JsonData> runSearch(SearchRequest request) {
        try {
            return client.search(request, JsonData.class);

        } catch(IOException e) {
            throw new InternalServerException("There was a problem connecting to ElasticSearch", e);
        } catch (ElasticsearchException e) {
            throw new IndexDoesNotExistException("There was a problem processing your request", e);
        }
    }

    private String getHits(SearchResponse<JsonData> response) {
        return response.hits().hits().stream().filter(x -> x.source() != null).map(x -> x.source().toJson()).toList().toString();
    }

    private String getAggs(SearchResponse<JsonData> response, String aggName) {
        var list = response.aggregations().get(aggName).sterms().buckets().array();

        var buckets = list.stream()
                .map(x -> Json.createObjectBuilder().add("key", x.key()).add("doc_count", x.docCount()).build());

        var result = Json.createArrayBuilder();

        buckets.forEach(result::add);

        return result.build().toString();
    }
}
