package co.empathy.academy.search.utils;

import co.empathy.academy.search.exception.types.InternalServerException;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;

public class SuggestionSearch {
    private static RestHighLevelClient client = ElasticHighClientUtils.getClient();
    private SuggestionSearch() {}

    private static void addTermSuggestion(String q, SuggestBuilder suggestBuilder) {
        SuggestionBuilder<TermSuggestionBuilder> termSuggestionBuilder =
                SuggestBuilders.termSuggestion("primaryTitle").text(q);

        suggestBuilder.addSuggestion("spellcheck", termSuggestionBuilder);
    }

    public static String run(String q) {
        var searchRequest = new SearchRequest("films");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();

        addTermSuggestion(q, suggestBuilder);

        addPhraseSuggestion(q, suggestBuilder);

        searchSourceBuilder.suggest(suggestBuilder);

        searchRequest.source(searchSourceBuilder);

        try {
            var suggestResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            TermSuggestion termSuggest = suggestResponse.getSuggest().getSuggestion("spellcheck");
            PhraseSuggestion phraseSuggest = suggestResponse.getSuggest().getSuggestion("phrase-suggester");

            var termArray = Json.createArrayBuilder();
            var phraseArray = Json.createArrayBuilder();

            addTermResults(termSuggest, termArray);
            addPhraseResults(phraseSuggest, phraseArray);

            return Json.createObjectBuilder()
                    .add("hits", Json.createArrayBuilder().build())
                    .add("aggs", Json.createArrayBuilder().build())
                    .add("term-suggestions", termArray.build())
                    .add("phrase-suggestions", phraseArray.build())
                    .build()
                    .toString();

        } catch (IOException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }

    private static void addPhraseResults(PhraseSuggestion suggest, JsonArrayBuilder phraseArray) {
        suggest.getEntries().get(0).getOptions()
                .stream().map(option ->
                        Json.createObjectBuilder()
                                .add("score", option.getScore())
                                .add("text", option.getText().toString())
                                .build())
                .forEach(phraseArray::add);
    }

    private static void addTermResults(TermSuggestion suggest, JsonArrayBuilder termArray) {
        suggest.getEntries().get(0).getOptions()
                .stream().map(option ->
                        Json.createObjectBuilder()
                                .add("score", option.getScore())
                                .add("freq", option.getFreq())
                                .add("text", option.getText().toString())
                                .build())
                .forEach(termArray::add);
    }

    private static void addPhraseSuggestion(String q, SuggestBuilder suggestBuilder) {
        SuggestionBuilder<PhraseSuggestionBuilder> phraseSuggestionBuilder =
                SuggestBuilders.phraseSuggestion("primaryTitle").text(q).gramSize(3).maxErrors(3);

        suggestBuilder.addSuggestion("phrase-suggester", phraseSuggestionBuilder);
    }
}
