package co.empathy.academy.search.utils.clases;

import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

import java.util.List;
import java.util.Map;

public class Principals {
    private static final int NCONST = 2;
    private static final int CHARACTERS = 5;

    private Principals() {}


    public static void addPrincipals(List<String> principalsLines, JsonObjectBuilder builder, List<String> akasHeaders, Map<String, String[]> nameBasics, List<String> nameHeaders) {
        var principalsArray = Json.createArrayBuilder();
        for(String principalLine : principalsLines) {
            var fields = principalLine.split("\t");

            principalsArray.add(Json.createObjectBuilder()
                    .add("name", Name.addName(fields[NCONST], nameBasics, nameHeaders))
                    .add(akasHeaders.get(CHARACTERS), fields[CHARACTERS])
            );
        }

        builder.add("principals", principalsArray);
    }
}
