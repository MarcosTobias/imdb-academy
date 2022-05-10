package co.empathy.academy.search.utils;

import co.empathy.academy.search.exception.types.InternalServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class TSVMerger {
    private static final Pattern DELIMITER = Pattern.compile("\t");
    private static final Logger logger = LoggerFactory.getLogger(TSVMerger.class);


    private TSVMerger(){}

    public static void sort(String filmsPath, String outputPath) {
        try(BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));) {
            List<String> films = Files.readAllLines(Path.of(filmsPath));

            var helpers = films.stream().skip(1).map(x -> new Helper(Integer.parseInt(x.split(DELIMITER.pattern())[0].split("tt")[1]), x))
                    .sorted((o1, o2) -> Integer.compare(o1.id, o2.id)).toList();

            outputWriter.write(films.get(0));
            outputWriter.newLine();

            for(Helper p : helpers) {
                outputWriter.write(p.content);
                outputWriter.newLine();
            }

            logger.info("Finished sorting");
        } catch (IOException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }

    public static void mergeFiles(String filmsPath, String ratingsPath, String outputPath) {
        try(
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));
                BufferedReader filmsReader = new BufferedReader(new FileReader(filmsPath));
                BufferedReader ratingsReader = new BufferedReader(new FileReader(ratingsPath));

        ) {
            String filmsLine = filmsReader.readLine();
            String ratingsLine = ratingsReader.readLine();
            outputWriter.write(filmsLine);
            outputWriter.write("\t" + Arrays.stream(ratingsLine.split(DELIMITER.pattern())).skip(1).reduce("", (res, element) -> res += element + "\t"));
            outputWriter.newLine();
            filmsLine = filmsReader.readLine();
            ratingsLine = ratingsReader.readLine();

            while(filmsLine != null && ratingsLine != null) {
                var filmLineArray = filmsLine.split(DELIMITER.pattern());
                var ratingLineArray = ratingsLine.split(DELIMITER.pattern());

                var filmIndex = Integer.valueOf(filmLineArray[0].split("tt")[1]);
                var ratingIndex = Integer.valueOf(ratingLineArray[0].split("tt")[1]);

                if(filmIndex.equals(ratingIndex) ) {
                    outputWriter.write(filmsLine);
                    outputWriter.write(Arrays.stream(ratingLineArray).skip(1).reduce("\t", (res, element) -> res += element + "\t"));
                    outputWriter.newLine();
                    filmsLine = filmsReader.readLine();
                    ratingsLine = ratingsReader.readLine();
                } else if(filmIndex < ratingIndex) {
                    outputWriter.write(filmsLine);
                    outputWriter.write("\t0.0\t0\t");
                    outputWriter.newLine();
                    filmsLine = filmsReader.readLine();
                } else {
                    ratingsLine = ratingsReader.readLine();
                }
            }

            if(filmsLine != null) {
                do {
                    outputWriter.write(filmsLine);
                    outputWriter.write("\t0.0\t0\t");
                    outputWriter.newLine();
                } while ((filmsLine = filmsReader.readLine()) != null);
            }

        } catch(IOException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }

        logger.info("Finished merging");
    }

    record Helper(int id, String content) {
    }
}
