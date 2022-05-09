package co.empathy.academy.search.utils.clases;

import co.empathy.academy.search.utils.StringToBoolConverter;
import co.empathy.academy.search.utils.StringToDoubleConverter;
import co.empathy.academy.search.utils.StringToIntConverter;
import jakarta.json.Json;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class Film {
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int AVERAGE_RATING = 9;
    private static final int NUM_VOTES = 10;
    private Film(){}

    public static JsonContent toJsonContent(String line, List<String> headers) {
        String[] fields = line.split("\t");

        var builder = Json.createObjectBuilder();
        int counter = 0;

        for(String field : fields) {
            var array = field.split(",");
            if(array.length == 1) {
                switch (counter) {
                    case IS_ADULT -> builder.add(headers.get(counter), StringToBoolConverter.getBool(field));
                    case START_YEAR, END_YEAR, RUNTIME_MINUTES, NUM_VOTES -> builder.add(headers.get(counter), StringToIntConverter.getInt(field));
                    case AVERAGE_RATING -> builder.add(headers.get(counter), StringToDoubleConverter.getDouble(field));
                    default -> builder.add(headers.get(counter), field);
                }

            } else {
                var arrayBuilder = Json.createArrayBuilder();

                Arrays.stream(array).forEach(arrayBuilder::add);

                builder.add(headers.get(counter), arrayBuilder);
            }

            counter++;
        }

        return new JsonContent(fields[0], builder.build());
    }
}
