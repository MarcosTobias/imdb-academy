package co.empathy.academy.search.utils.clases;

import co.empathy.academy.search.utils.StringToBoolConverter;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

import java.util.List;

public class Akas {
    private static final int TITLE = 2;
    private static final int REGION = 3;
    private static final int LANGUAGE = 4;
    private static final int IS_ORIGINAL_TITLE = 7;
    private Akas(){}

    public static void addAkas(List<String> lines, JsonObjectBuilder builder, List<String> headers) {
        var arrayBuilder = Json.createArrayBuilder();

        for(String line : lines) {
            var fields = line.split("\t");
            var lineJson = Json.createObjectBuilder()
                    .add(headers.get(TITLE), fields[TITLE])
                    .add(headers.get(REGION), fields[REGION])
                    .add(headers.get(LANGUAGE), fields[LANGUAGE])
                    .add(headers.get(IS_ORIGINAL_TITLE), StringToBoolConverter.getBool(fields[IS_ORIGINAL_TITLE]))
                    .build();

            arrayBuilder.add(lineJson);
        }

        builder.add("akas", arrayBuilder.build());
    }
}
