package co.empathy.academy.search.utils;

import co.empathy.academy.search.exception.types.InternalServerException;
import jakarta.json.Json;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;

public class SuggestionSearch {
    private static RestHighLevelClient client = ElasticHighClientUtils.getClient();
    private SuggestionSearch() {}

    public static String run(String q) {
        var searchRequest = new SearchRequest("films");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder<TermSuggestionBuilder> termSuggestionBuilder =
                SuggestBuilders.termSuggestion("primaryTitle").text(q);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("spellcheck", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);

        searchRequest.source(searchSourceBuilder);

        try {
            var suggestResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            TermSuggestion suggest = suggestResponse.getSuggest().getSuggestion("spellcheck");

            var result = Json.createObjectBuilder();

            var optionArray = Json.createArrayBuilder();

            suggest.getEntries().get(0).getOptions()
                    .stream().map(option ->
                            Json.createObjectBuilder()
                                    .add("score", option.getScore())
                                    .add("freq", option.getFreq())
                                    .add("text", option.getText().toString())
                                    .build())
                    .forEach(optionArray::add);

            result.add("hits", Json.createArrayBuilder().build());
            result.add("aggs", Json.createArrayBuilder().build());
            result.add("term-suggestions", optionArray.build());

            return result.build().toString();

        } catch (IOException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }
}
