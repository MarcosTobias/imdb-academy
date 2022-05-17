package co.empathy.academy.search.utils.clases;

import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Crew {
    private static final int DIRECTORS = 1;
    private Crew() {}

    public static void addCrews(String line, JsonObjectBuilder builder, List<String> headers, Map<String, String[]> nameBasics, List<String> nameHeaders) {
        var directorsArray = Json.createArrayBuilder();

        var fields = line.split("\t");

        String[] directorIds = fields[DIRECTORS].split(",");

        Arrays.stream(directorIds).map(x -> Name.addName(x, nameBasics, nameHeaders)).forEach(directorsArray::add);

        builder.add(headers.get(DIRECTORS), directorsArray);
    }
}
