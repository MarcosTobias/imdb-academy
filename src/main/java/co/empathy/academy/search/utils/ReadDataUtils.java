package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.search.exception.types.InternalServerException;
import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used for reading data from the file specified on batches
 */
public class ReadDataUtils {
    private final ElasticsearchClient client = ElasticUtils.getClient();
    Map<String, JsonObject> ratings = new HashMap<>();
    //Size of the batches
    private int batchSize = 25000;
    //Reader for the file
    private String filePath = "";
    private String ratingsFilePath = "";

    /**
     * Creates a ReadDataUtils object
     * @param filePath of the file that is wanted to be read
     * @param batchSize size of the batches to be used
     */
    public ReadDataUtils(String filePath, int batchSize) {
        this(filePath);
        this.batchSize= batchSize;
    }

    /**
     * Creates a ReadDataUtils object
     * @param filePath of the file that is wanted to be read
     */
    public ReadDataUtils(String filePath) {
        this.filePath = filePath;
    }

    public ReadDataUtils(String filePath, String ratingsFilePath) {
        this.filePath = filePath;
        this.ratingsFilePath = ratingsFilePath;
    }

    /**
     * Returns a JsonContent consisting on the id of the specified line, and the value
     * being a JSON with the different fields of the line parsed
     * @param line containing the document that we want to index
     * @return a JsonContent with the id and the document in JSON format
     */
    private JsonContent getJson(String line) {
        String[] fields = line.split("\t");

        var array = Json.createArrayBuilder();

        Arrays.stream(fields[8].split(",")).forEach(array::add);

        var builder = Json.createObjectBuilder()
                .add("titleType", fields[1])
                .add("primaryTitle", fields[2])
                .add("originalTitle", fields[3])
                .add("isAdult", StringToBoolConverter.getBool(fields[4]))
                .add("startYear", StringToIntConverter.getInt(fields[5]))
                .add("endYear", StringToIntConverter.getInt(fields[6]))
                .add("runtimeMinutes", StringToIntConverter.getInt(fields[7]))
                .add("genres", array.build());

        if(!this.ratingsFilePath.isEmpty()) {
            var jsonObject = this.ratings.get(fields[0]);
            if(jsonObject == null) {
                builder.add("averageRating", 0.0)
                        .add("numVotes", 0);
            } else {
                builder.add("averageRating", jsonObject.get("averageRating"))
                        .add("numVotes", jsonObject.get("numVotes"));
            }
        }

        return new JsonContent(fields[0], builder.build());
    }

    private void getMap(List<String> ratings) {
        Map<String, JsonObject> map = new HashMap<>();


        ratings.forEach(x -> {
            String[] fields = x.split("\t");

            map.put(fields[0], Json.createObjectBuilder()
                    .add("averageRating", StringToDoubleConverter.getDouble(fields[1]))
                    .add("numVotes", StringToIntConverter.getInt(fields[2]))
                    .build()
            );
        });

        this.ratings = map;
    }

    private void getRatings() {
        try {
            List<String> linesRatings = Files.readAllLines(Path.of(this.ratingsFilePath));

            getMap(linesRatings.subList(1, linesRatings.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Index the data
     */
    public void indexData() {
        try {
            System.out.println("Started indexing");
            List<String> lines = Files.readAllLines(Path.of(this.filePath));

            if(!this.ratingsFilePath.isEmpty()) {
                getRatings();
            }

            System.out.println("Read");

            long current = 1;
            long localBatch = this.batchSize;

            if(this.batchSize > lines.size()) localBatch = lines.size() - 1L;

            while(current < lines.size() - 1L) {
                if (current + batchSize > lines.size())
                    localBatch = lines.size() - current;

                long finalCurrent = current;
                long finalLocalBatch = localBatch;

                client.bulk(_0 -> _0
                        .operations(lines.stream().skip(finalCurrent).limit(finalLocalBatch)
                                .map(this::getJson)
                                .map(x ->
                                        BulkOperation.of(_1 -> _1
                                                .index(_2 -> _2
                                                        .index("films")
                                                        .document(x.json)
                                                        .id(x.id)
                                                )
                                        )
                                ).toList())
                );


                System.out.println("Indexed");
                current += batchSize;
            }
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }

    private static class JsonContent {
        protected String id;
        protected JsonObject json;

        public JsonContent(String id, JsonObject json) {
            this.id = id;
            this.json = json;
        }
    }
}
