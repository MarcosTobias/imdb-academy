package co.empathy.academy.search.utils.clases;

import co.empathy.academy.search.utils.StringToBoolConverter;
import co.empathy.academy.search.utils.StringToIntConverter;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class Film {
    private static final int TCONST = 0;
    private static final int TITLE_TYPE = 1;
    private static final int PRIMARY_TITLE = 2;
    private static final int ORIGINAL_TITLE = 3;
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int GENRES = 8;
    private Film(){}

    public static void addFilm(String line, JsonObjectBuilder builder, List<String> headers) {
        String[] fields = line.split("\t");

        var arrayBuilder = Json.createArrayBuilder();

        Arrays.stream(fields[GENRES].split(",")).forEach(arrayBuilder::add);

        builder.add(headers.get(TCONST), fields[TCONST])
                .add(headers.get(TITLE_TYPE), fields[TITLE_TYPE])
                .add(headers.get(PRIMARY_TITLE), fields[PRIMARY_TITLE])
                .add(headers.get(ORIGINAL_TITLE), fields[ORIGINAL_TITLE])
                .add(headers.get(IS_ADULT), StringToBoolConverter.getBool(fields[IS_ADULT]))
                .add(headers.get(START_YEAR), StringToIntConverter.getInt(fields[START_YEAR]))
                .add(headers.get(END_YEAR), StringToIntConverter.getInt(fields[END_YEAR]))
                .add(headers.get(RUNTIME_MINUTES), StringToIntConverter.getInt(fields[RUNTIME_MINUTES]))
                .add(headers.get(GENRES), arrayBuilder.build());
    }
}
