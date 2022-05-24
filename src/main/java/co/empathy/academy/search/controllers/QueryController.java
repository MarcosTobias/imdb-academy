package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.empathy.academy.search.exception.types.IndexDoesNotExistException;
import co.empathy.academy.search.exception.types.InternalServerException;
import co.empathy.academy.search.utils.ElasticUtils;
import co.empathy.academy.search.utils.SuggestionSearch;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
    @Parameter(name = "q", description = "String with the query. Mandatory field")
    @Parameter(name = "type", description = "Title type. It must match exactly. Can be one or more, separated by commas")
    @Parameter(name = "genre", description = "Genre of the field. It must match exactly. Can be one or more, separated by commas")
    @Parameter(name = "agg", description = "Field for aggregating the query. It must match exactly")
    @Parameter(name = "gte", description = "Specify a value and only films with higher averageRating will be shown. Expects number with a decimal")
    @Parameter(name = "from", description = "Number of hits that is going to be skipped")
    @Parameter(name = "size", description = "Size of hits to be returned")
    @Parameter(name = "directorId", description = "Id of the director for getting his films")
    @ApiResponse(responseCode = "200", description="Documents obtained", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "400", description="Wrong request", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "500", description="Server Internal Error", content = { @Content(mediaType= "application/json")})
    @GetMapping("/search")
    public String search(
            @RequestParam String q,
            @RequestParam(required = false) Optional<List<String>> type,
            @RequestParam(required = false) Optional<List<String>> genre,
            @RequestParam(required = false) Optional<String> agg,
            @RequestParam(required = false) Optional<String> gte,
            @RequestParam(required = false) Optional<String> from,
            @RequestParam(required = false) Optional<String> size,
            @RequestParam(required = false) Optional<String> directorId) {

        var request = new SearchRequest.Builder().index("films");

        from.ifPresent(f -> request.from(Integer.valueOf(f)));
        size.ifPresent(s -> request.size(Integer.valueOf(s)));

        var boolQuery = new BoolQuery.Builder();

        addSearch(q, boolQuery);

        type.ifPresent(strings -> addTermFilter("titleType", strings, boolQuery));

        genre.ifPresent(strings -> addTermFilter("genres", strings, boolQuery));

        gte.ifPresent(s -> addRangeFilter(s, boolQuery));

        removeAdultFilms(boolQuery);

        var functionScoreQuery = getFunctionScoreQuery(boolQuery);

        request.query(_0 -> _0.functionScore(functionScoreQuery));

        directorId.ifPresent(i -> addDirectorFilter(i, boolQuery));

        agg.ifPresent(s -> addAgg(s, request));

        var response = runSearch(request.build());

        if(response.hits().hits().isEmpty()) {
            return SuggestionSearch.run(q);
        } else {
            return getResult(response, agg.orElse(null));
        }
    }

    @Operation(summary = "Retrieves the document with the specified index")
    @Parameter(name = "id", description = "Id of the document. Mandatory field")
    @ApiResponse(responseCode = "200", description="Documents obtained", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "400", description="Wrong request", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "500", description="Server Internal Error", content = { @Content(mediaType= "application/json")})
    @GetMapping("/id_search")
    public String idSearch(@RequestParam String id) {
        var request = new SearchRequest.Builder().index("films");

        request.query(_0 -> _0
                .match(_1 -> _1
                        .field("_id")
                        .query(id)
                )
        );

        var response = runSearch(request.build());

        return getResult(response, null);
    }

    private FunctionScoreQuery getFunctionScoreQuery(BoolQuery.Builder boolQuery) {
        var nVotesValueFactor = FieldValueFactorScoreFunction.of(_0 -> _0
                .field("numVotes")
                .missing(0.1)
                .modifier(FieldValueFactorModifier.Log1p)
                .factor(2D)
        );

        var scoreValueFactor = FieldValueFactorScoreFunction.of(_0 -> _0
                .field("averageRating")
                .missing(0.1)
                .modifier(FieldValueFactorModifier.Ln1p)
                .factor(2D)
        );

        return FunctionScoreQuery.of(_0 -> _0
                .query(boolQuery.build()._toQuery())
                .functions(Stream.of(nVotesValueFactor, scoreValueFactor)
                        .map(x -> FunctionScore.of(_1 -> _1.fieldValueFactor(x)))
                        .toList())
                .scoreMode(FunctionScoreMode.Multiply)
                .boostMode(FunctionBoostMode.Multiply)
        );
    }

    private void addRangeFilter(String averageRating, BoolQuery.Builder boolQuery) {
        boolQuery
                .filter(_0 -> _0
                        .range(_1 -> _1
                                .field("averageRating")
                                .gte(JsonData.of(averageRating))
                        )
                );
    }

    private void addDirectorFilter(String directorId, BoolQuery.Builder boolQuery) {
        boolQuery.filter(_0 -> _0
                .nested(_1 -> _1
                        .path("directors")
                        .query(_2 -> _2
                                .match(_3 -> _3
                                        .field("directors.nconst")
                                        .query(directorId)
                                )
                        )
                )
        );
    }

    private void addSearch(String q, BoolQuery.Builder boolQuery) {
        if (!q.isEmpty()) {
            addNormalSearch(q, boolQuery);
        } else {
            matchAllSearch(boolQuery);
        }
    }

    private void matchAllSearch(BoolQuery.Builder boolQuery) {
        boolQuery.must(_0 -> _0
                .matchAll(_1 -> _1)
        );
    }


    private void addNormalSearch(String q, BoolQuery.Builder boolQuery) {

        var movieQuery = TermQuery.of(_0 -> _0
                .field("titleType")
                .value("movie")
                .boost(20F)
        )._toQuery();

        var primaryKeywordQuery = TermQuery.of(_0 -> _0
                .field("primaryTitle.raw")
                .value(q)
                .boost(10F)
        )._toQuery();

        var primaryEnglishQuery = MatchQuery.of(_0 -> _0
                .field("primaryTitle.english")
                .query(q)
                .boost(2F)
        )._toQuery();

        var primaryAndQuery = MatchQuery.of(_0 -> _0
                .field("primaryTitle")
                .query(q)
                .operator(Operator.And)
                .boost(4F)
        )._toQuery();

        var primaryPhraseQuery = MatchPhraseQuery.of(_0 -> _0
                .field("primaryTitle")
                .query(q)
                .boost(9F)
        )._toQuery();

        boolQuery.should(movieQuery, primaryKeywordQuery, primaryEnglishQuery, primaryAndQuery);
        boolQuery.must(primaryPhraseQuery);
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

    private void removeAdultFilms(BoolQuery.Builder boolQuery) {
        boolQuery
                .filter(_2 -> _2
                        .term(_3 -> _3
                                .field("isAdult")
                                .value(false)
                        )
                );
    }

    private void addAgg(String agg, SearchRequest.Builder request) {
        request
                .size(0)
                .aggregations(agg + "_agg", _0 -> _0
                        .terms(_1 -> _1
                                .field(agg)
                                .size(1000)
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

    private JsonArrayBuilder getHits(SearchResponse<JsonData> response) {
        var hitArray = Json.createArrayBuilder();

        response.hits().hits().stream().filter(x -> x.source() != null).map(x ->
                Json.createObjectBuilder()
                        .add("id", x.id())
                        .add("source", x.source().toJson())
                        .build()
        ).toList().forEach(hitArray::add);

        return hitArray;
    }

    private String getResult(SearchResponse<JsonData> response, String aggName)  {
        var result = Json.createObjectBuilder();
        result.add("hits", getHits(response));

        if(aggName != null)
            result.add("aggs", getAggs(response, aggName));

        return result.build().toString();
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
