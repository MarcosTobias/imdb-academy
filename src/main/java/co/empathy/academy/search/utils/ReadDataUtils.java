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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Used for reading data from the file specified on batches
 */
public class ReadDataUtils {
    private final int MAX_THREADS = 3;
    //Size of the batches
    private int batchSize = 50000;
    //Reader for the file
    private String filePath = "";

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

    /**
     * Index the data
     */
    public void indexData() {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Path.of(this.filePath));
        } catch (IOException e) {
            throw new InternalServerException("There was a problem reading the file you provided", e);
        }

        int workersSubListSizes = lines.size() / MAX_THREADS;

        ExecutorService pool = Executors.newFixedThreadPool(MAX_THREADS);

        int workerStartCounter = 1;

        for(int i = 0; i < MAX_THREADS; i++) {
            if(i == MAX_THREADS - 1)
                pool.execute(new WorkerBulkIndexer(lines.subList(workerStartCounter, lines.size()), this.batchSize));
            else
                pool.execute(new WorkerBulkIndexer(lines.subList(workerStartCounter, workerStartCounter + workersSubListSizes), this.batchSize));
            workerStartCounter+= workersSubListSizes;
        }

        pool.shutdown();
    }

    private static class JsonContent {
        protected String id;
        protected JsonObject json;

        public JsonContent(String id, JsonObject json) {
            this.id = id;
            this.json = json;
        }
    }

    private static class WorkerBulkIndexer implements Runnable {
        private final ElasticsearchClient client = ElasticUtils.getClient();
        private List<String> lines = null;
        private int batchSize = 0;
        public WorkerBulkIndexer(List<String> lines, int batchSize) {
            this.lines = lines;
            this.batchSize = batchSize;
        }

        /**
         * Returns a JsonContent consisting on the id of the specified line, and the value
         * being a JSON with the different fields of the line parsed
         * @param line containing the document that we want to index
         * @return a JsonContent with the id and the document in JSON format
         */
        private JsonContent getJson(String line) {
            String[] fields = line.split("\t");


            return new JsonContent(fields[0], Json.createObjectBuilder()
                    .add("titleType", fields[1])
                    .add("primaryTitle", fields[2])
                    .add("originalTitle", fields[3])
                    .add("isAdult", StringToBoolConverter.getBool(fields[4]))
                    .add("startYear", StringToIntConverter.getInt(fields[5]))
                    .add("endYear", StringToIntConverter.getInt(fields[6]))
                    .add("runtimeMinutes", StringToIntConverter.getInt(fields[7]))
                    .add("genres", fields[8])
                    .build());
        }

        @Override
        public void run() {
            try {
                long current = 0;
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
                                                            .id(x.id)
                                                            .document(x.json)
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
    }
}
