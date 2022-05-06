package co.empathy.academy.search.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.search.exception.types.InternalServerException;
import co.empathy.academy.search.utils.clases.Film;
import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used for reading data from the file specified on batches
 */
public class ReadDataUtils {
    //Size of the batches
    private static final int BATCH_SIZE = 25000;
    private final ElasticsearchClient client = ElasticUtils.getClient();

    private Map<String, JsonObject> getMap(List<String> ratings) {
        Map<String, JsonObject> map = new HashMap<>();


        ratings.forEach(x -> {
            String[] fields = x.split("\t");

            map.put(fields[0], Json.createObjectBuilder()
                    .add("averageRating", StringToDoubleConverter.getDouble(fields[1]))
                    .add("numVotes", StringToIntConverter.getInt(fields[2]))
                    .build()
            );
        });

        return map;
    }
    /**
     * Index the data
     */
    public void indexData(String filmsPath, String ratingsPath) {
        try {
            System.out.println("Started indexing");
            List<String> lines = Files.readAllLines(Path.of(filmsPath));

            List<String> linesRatings = Files.readAllLines(Path.of(ratingsPath));

            var ratingsMap = getMap(linesRatings.subList(1, linesRatings.size()));

            System.out.println("Read");

            long current = 1;
            long localBatch = BATCH_SIZE;

            if(BATCH_SIZE > lines.size()) localBatch = lines.size() - 1L;

            while(current < lines.size() - 1L) {
                if (current + BATCH_SIZE > lines.size())
                    localBatch = lines.size() - current;

                long finalCurrent = current;
                long finalLocalBatch = localBatch;

                client.bulk(_0 -> _0
                        .operations(lines.stream().skip(finalCurrent).limit(finalLocalBatch)
                                .map(x -> new Film(x, ratingsMap).toJsonContent())
                                .map(x ->
                                        BulkOperation.of(_1 -> _1
                                                .index(_2 -> _2
                                                        .index("films")
                                                        .document(x.json())
                                                        .id(x.id())
                                                )
                                        )
                                ).toList())
                );


                System.out.println("Indexed");
                current += BATCH_SIZE;
            }
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }
}
