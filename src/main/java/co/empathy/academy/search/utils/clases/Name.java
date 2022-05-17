package co.empathy.academy.search.utils.clases;

import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.util.List;
import java.util.Map;

public class Name {
    private static final int NCONST = 0;
    private static final int PRIMARY_NAME = 1;
    private static final int KNOWN_FOR_TITLES = 5;
    private Name(){}

    public static JsonObject addName(String id, Map<String, String[]> nameBasics, List<String> nameHeaders) {
        return Json.createObjectBuilder()
                .add("nconst", id)
                .build();

        //Names dont fit in the heap
        //TODO change names to index them separately
        /*var fields = nameBasics.get(id);

        var knownTitlesArray = Json.createArrayBuilder();

        var knownTitles = fields[KNOWN_FOR_TITLES].split(",");
        Arrays.stream(knownTitles).forEach(knownTitlesArray::add);


        var result = Json.createObjectBuilder()
                .add(nameHeaders.get(NCONST), fields[NCONST])
                .add(nameHeaders.get(PRIMARY_NAME), fields[PRIMARY_NAME])
                .add(nameHeaders.get(KNOWN_FOR_TITLES), knownTitlesArray);

        return result.build();*/
    }
}
