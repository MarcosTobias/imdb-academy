package co.empathy.academy.search.utils;

import co.empathy.academy.search.exception.types.InternalServerException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TSVMerger {
    private static final Pattern DELIMITER = Pattern.compile("\t");

    private TSVMerger(){}

    public static void mergeFiles(String filmsPath, String ratingsPath, String outputPath) {
        try(BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath))) {
            List<String> films = Files.readAllLines(Path.of(filmsPath));
            List<String> ratings = Files.readAllLines(Path.of(ratingsPath));

            var filmsMap = films.stream().skip(1).map(DELIMITER::split)
                    .collect(Collectors.toMap(a -> a[0], a -> a));

            var ratingsMap = ratings.stream().skip(1).map(DELIMITER::split)
                    .collect(Collectors.toMap(a -> a[0], a -> Arrays.copyOfRange(a, 1, a.length)));

            outputWriter.write(films.get(0) + "\t");
            outputWriter.write(Arrays.stream(ratings.get(0).split(DELIMITER.pattern())).skip(1).reduce("", (res, element) -> res += element + "\t"));
            outputWriter.newLine();

            
            filmsMap.forEach((key, value) -> {
                try {
                outputWriter.write(Arrays.stream(value).reduce("", (res, element) -> res += element + "\t"));

                    if(ratingsMap.containsKey(key)) {
                            outputWriter.write(Arrays.stream(ratingsMap.get(key)).reduce("", (res, element) -> res += element + "\t"));
                    }

                outputWriter.newLine();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch(IOException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }
}
