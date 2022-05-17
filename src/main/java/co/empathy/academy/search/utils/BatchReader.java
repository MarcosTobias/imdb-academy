package co.empathy.academy.search.utils;

import co.empathy.academy.search.utils.clases.*;
import jakarta.json.Json;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class BatchReader {
    private static final Pattern DELIMITER = Pattern.compile("\t");
    private final BufferedReader filmsReader;
    private final BufferedReader ratingsReader;
    private final BufferedReader akasReader;
    private final BufferedReader crewReader;
    private final BufferedReader episodesPath;
    private final BufferedReader principalReader;
    private final Map<String, String[]> nameBasics;
    private final int batchSize;
    private boolean hasFinished;
    private List<String> filmsHeaders;
    private List<String> ratingsHeaders;
    private List<String> akasHeaders;
    private List<String> crewHeaders;
    private List<String> episodesHeaders;
    private List<String> principalHeaders;
    private List<String> nameHeaders;

    public BatchReader(String filmsPath, String ratingsPath, String akasPath, String crewPath, String episodesPath, String principalPath, String nameBasicsPath, int batchSize) throws IOException {
        this.filmsReader = new BufferedReader(new FileReader(filmsPath));
        this.ratingsReader = new BufferedReader(new FileReader(ratingsPath));
        this.akasReader = new BufferedReader(new FileReader(akasPath));
        this.crewReader = new BufferedReader(new FileReader(crewPath));
        this.episodesPath = new BufferedReader(new FileReader(episodesPath));
        this.principalReader = new BufferedReader(new FileReader(principalPath));
        //nameBasics = Files.readAllLines(Path.of(nameBasicsPath)).stream().map(DELIMITER::split)
         //       .collect(Collectors.toMap(a -> a[0], a -> a));
        nameBasics = null;
        this.batchSize = batchSize;
        this.hasFinished = false;

        skipFirstLines();
    }

    public boolean hasFinished() {
        return this.hasFinished;
    }

    public void skipFirstLines() throws IOException {
        this.filmsHeaders = Arrays.stream(this.filmsReader.readLine().split(DELIMITER.pattern())).toList();
        this.ratingsHeaders = Arrays.stream(this.ratingsReader.readLine().split(DELIMITER.pattern())).toList();
        this.akasHeaders = Arrays.stream(this.akasReader.readLine().split(DELIMITER.pattern())).toList();
        this.crewHeaders = Arrays.stream(this.crewReader.readLine().split(DELIMITER.pattern())).toList();
        this.episodesHeaders = Arrays.stream(this.episodesPath.readLine().split(DELIMITER.pattern())).toList();
        this.principalHeaders = Arrays.stream(this.principalReader.readLine().split(DELIMITER.pattern())).toList();
        //this.nameHeaders = Arrays.stream(nameBasics.get("nconst")).toList();
        this.nameHeaders = null;
    }

    public void close() throws IOException {
        this.filmsReader.close();
        this.ratingsReader.close();
        this.akasReader.close();
        this.crewReader.close();
        this.episodesPath.close();
        this.principalReader.close();
    }

    public List<JsonContent> getBatch() throws IOException {
        int counter = 0;

        List<JsonContent> result = new ArrayList<>();

        while(counter < batchSize) {
            var builder = Json.createObjectBuilder();

            String filmLine = filmsReader.readLine();

            if(filmLine == null) {
                this.hasFinished = true;
                return result;
            }

            int currentId = Integer.parseInt(filmLine.split(DELIMITER.pattern())[0].split("tt")[1]);

            Film.addFilm(filmLine, builder, filmsHeaders);

            String ratingLine = ratingsReader.readLine();
            Rating.addRating(ratingLine, builder, ratingsHeaders);

            List<String> akasLines = getLinesAndResetReader(akasReader, currentId);
            Akas.addAkas(akasLines, builder, akasHeaders);

            String crewLine = crewReader.readLine();
            Crew.addCrews(crewLine, builder, crewHeaders, nameBasics, nameHeaders);

            List<String> principalsLines = getLinesAndResetReader(principalReader, currentId);
            Principals.addPrincipals(principalsLines, builder, principalHeaders, nameBasics, nameHeaders);

            result.add(new JsonContent(filmLine.split(DELIMITER.pattern())[0], builder.build()));
            counter++;
        }

        return result;
    }


    private List<String> getLinesAndResetReader(BufferedReader reader, int currentId) throws IOException {
        boolean nextId = false;
        List<String> lines = new ArrayList<>();

        while(!nextId) {
            reader.mark(10000);
            String line = reader.readLine();

            if(line == null)
                nextId = true;
            else {
                int id = Integer.parseInt(line.split(DELIMITER.pattern())[0].split("tt")[1]);

                if (id > currentId)
                    nextId = true;
                else
                    lines.add(line);
            }
        }

        reader.reset();

        return lines;
    }
}
