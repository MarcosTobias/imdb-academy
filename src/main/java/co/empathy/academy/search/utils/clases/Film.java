package co.empathy.academy.search.utils.clases;

import co.empathy.academy.search.utils.StringToBoolConverter;
import co.empathy.academy.search.utils.StringToIntConverter;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.Map;

@Getter
@Setter
public class Film {
    private static final int ID = 0;
    private static final int TITLE_TYPE = 1;
    private static final int PRIMARY_TITLE = 2;
    private static final int ORIGINAL_TITLE = 3;
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int GENRES = 8;
    private static final int NU_VOTES = 1;
    private static final int AVERAGE_RATING = 1;

    private String line;
    private Map<String, JsonObject> ratingsMap;

    public Film(String line, Map<String, JsonObject> ratingsMap) {
        this.line = line;
        this.ratingsMap = ratingsMap;
    }

    public JsonContent toJsonContent() {
        String[] fields = this.line.split("\t");

        var array = Json.createArrayBuilder();

        Arrays.stream(fields[GENRES].split(",")).forEach(array::add);

        var builder = Json.createObjectBuilder()
                .add("titleType", fields[TITLE_TYPE])
                .add("primaryTitle", fields[PRIMARY_TITLE])
                .add("originalTitle", fields[ORIGINAL_TITLE])
                .add("isAdult", StringToBoolConverter.getBool(fields[IS_ADULT]))
                .add("startYear", StringToIntConverter.getInt(fields[START_YEAR]))
                .add("endYear", StringToIntConverter.getInt(fields[END_YEAR]))
                .add("runtimeMinutes", StringToIntConverter.getInt(fields[RUNTIME_MINUTES]))
                .add("genres", array.build());

        var jsonObject = ratingsMap.get(fields[ID]);

        if(jsonObject == null) {
            builder.add("averageRating", 0.0)
                    .add("numVotes", 0);
        } else {
            builder.add("averageRating", jsonObject.get("averageRating"))
                    .add("numVotes", jsonObject.get("numVotes"));
        }

        return new JsonContent(fields[0], builder.build());
    }
}
