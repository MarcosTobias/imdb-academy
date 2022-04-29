package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
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
    @Parameter(name = "q", description = "String with the query")
    @Parameter(name = "type", description = "Title type. It must match exactly. Can be one or more, separated by commas")
    @Parameter(name = "genre", description = "Genre of the field. It must match exactly. Can be one or more, separated by commas")
    @Parameter(name = "agg", description = "Field for aggregating the query. It must match exactly")
    @Parameter(name = "gte", description = "Specify a value and only films with higher averageRating will be shown. Expects number with a decimal")
    @Parameter(name = "from", description = "Number of hits that is going to be skipped")
    @Parameter(name = "size", description = "Size of hits to be returned")
    @ApiResponse(responseCode = "200", description="Documents obtained", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "400", description="Wrong request", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "500", description="Server Internal Error", content = { @Content(mediaType= "application/json")})
    @GetMapping("/search")
    public String search(
            @RequestParam(required = false) Optional<String> q,
            @RequestParam(required = false) Optional<List<String>> type,
            @RequestParam(required = false) Optional<List<String>> genre,
            @RequestParam(required = false) Optional<String> agg,
            @RequestParam(required = false) Optional<String> gte,
            @RequestParam(required = false) Optional<String> from,
            @RequestParam(required = false) Optional<String> size) {

        var request = new SearchRequest.Builder().index("films");

        from.ifPresent(f -> request.from(Integer.valueOf(f)));
        size.ifPresent(s -> request.size(Integer.valueOf(s)));

        var boolQuery = new BoolQuery.Builder();

        q.ifPresent(s -> addSearch(s, boolQuery));

        type.ifPresent(strings -> addTermFilter("titleType", strings, boolQuery));

        genre.ifPresent(strings -> addTermFilter("genres", strings, boolQuery));

        gte.ifPresent(s -> addRangeFilter("averageRating", s, boolQuery));

        request.query(_0 -> _0.bool(boolQuery.build()));

        agg.ifPresent(s -> addAgg(s, request));

        addSort(request);

        var response = runSearch(request.build());

        return agg.isPresent() ? getAggs(response, agg.get() + "_agg") : getHits(response);
    }

    private void addSort(SearchRequest.Builder request) {
        request.sort(_0 -> _0
                .field(_1 -> _1
                        .field("averageRating")
                        .order(SortOrder.Desc)
                )
        );
    }

    private void addRangeFilter(String field, String averageRating, BoolQuery.Builder boolQuery) {
        boolQuery
                .filter(_0 -> _0
                        .range(_1 -> _1
                                .field(field)
                                .gte(JsonData.of(averageRating))
                        )
                );
    }

    private void addSearch(String q, BoolQuery.Builder boolQuery) {
        boolQuery
                .must(_3 -> _3
                        .multiMatch(_4 -> _4
                                .fields("primaryTitle", "originalTitle")
                                .query(q)
                                .fuzziness("2")
                        )
                );
    }

    private void addTermFilter(String fieldName, List<String> filter, BoolQuery.Builder boolQuery) {
        boolQuery
                .filter(_2 -> _2
                        .terms(_3 -> _3
                                .field(fieldName)
                                .terms(_4 -> _4
                                        .value(filter.stream().map(FieldValue::of).toList()
                                        )
                                )
                        )
                );
    }

    private void addAgg(String agg, SearchRequest.Builder request) {
        request
                .size(0)
                .aggregations(agg + "_agg", _0 -> _0
                        .terms(_1 -> _1
                                .field(agg)
                        )
                );
    }

    private SearchResponse<JsonData> runSearch(SearchRequest request) {
        try {
            var a = client.search(request, JsonData.class);

            return a;

        } catch(IOException e) {
            throw new InternalServerException("There was a problem connecting to ElasticSearch", e);
        } catch (ElasticsearchException e) {
            throw new IndexDoesNotExistException("There was a problem processing your request", e);
        }
    }

    private String getHits(SearchResponse<JsonData> response) {
        return response.hits().hits().stream().filter(x -> x.source() != null).map(x ->
                Json.createObjectBuilder()
                        .add("id", x.id())
                        .add("source", x.source().toJson())
                        .build()
                ).toList().toString();
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
