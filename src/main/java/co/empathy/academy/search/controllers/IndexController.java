package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.search.exception.types.IndexAlreadyExistsException;
import co.empathy.academy.search.exception.types.IndexDoesNotExistException;
import co.empathy.academy.search.exception.types.InternalServerException;
import co.empathy.academy.search.utils.ElasticUtils;
import co.empathy.academy.search.utils.ReadDataUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Private controller used for creating indices and adding documents
 */
@Tag(name = "Private controller", description = "Allows to create and remove indexes. Also for indexing documents")
@RestController
@RequestMapping("/admin/api")
public class IndexController {
    private final ElasticsearchClient client = ElasticUtils.getClient();

    /**
     * Returns the name of the indices currently stored on the elastic container
     * @return Json with the names
     */
    @Operation(summary = "Returns a list with the current indices")
    @ApiResponse(responseCode = "200", description="Indices obtained", content = { @Content(mediaType= "application/json")})
    @GetMapping("/_cat/indices")
    public Map<String, IndexState> getIndices() {
        try {
            return client.indices().get(c -> c.index("*")).result();

        } catch(IOException e) {
            e.printStackTrace();
        }

        return Collections.emptyMap();
    }

    /**
     * Create an index with the name provided
     * @param index name of the index to be created
     * @return true if the index has been created correctly, false otherwise
     */
    @Operation(summary = "Creates an index with the name provided")
    @ApiResponse(responseCode = "200", description = "Index created", content = { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "Index already exists", content = { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Problems connecting to ElasticSearch", content = { @Content(mediaType = "application/json")})
    @Parameter(name = "index", description = "Name of the index we want to create", required = true)
    @PutMapping("/{index}")
    public boolean createIndex(@PathVariable String index) {
        try {
            return Boolean.TRUE.equals(client.indices().create(c -> c.index(index)).acknowledged());

        } catch (IOException e) {
            throw new InternalServerException("There was a problem connecting to ElasticSearch", e);
        } catch (ElasticsearchException e) {
            throw new IndexAlreadyExistsException("Index '" + index + "' already exists", e);
        }
    }

    /**
     * Remove the index with the name provided
     * @param index name of the index to remove
     * @return true if the index has been deleted successfully, false otherwise
     */
    @Operation(summary = "Removes the index with the name provided")
    @ApiResponse(responseCode = "200", description = "Index removed", content = { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "Index does not exist", content = { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Problems connecting to ElasticSearch", content = { @Content(mediaType = "application/json")})
    @Parameter(name = "index", description = "Name of the index we want to remove", required = true)
    @DeleteMapping("/{index}")
    public boolean deleteIndex(@PathVariable String index) {
        try {
            return client.indices().delete(c -> c.index(index)).acknowledged();
        } catch(IOException e) {
            throw new InternalServerException("There was a problem connecting to ElasticSearch", e);
        } catch (ElasticsearchException e) {
            throw new IndexDoesNotExistException("Index '" + index + "' does not exist", e);
        }
    }

    /**
     * Removes and creates the films index for resetting purposes.
     * Then, id adds the mapping for the index
     * Finally, it indexes every document stored in the title.basics.tsv file on the resources folder
     */
    @Operation(summary = "Creates the field index, puts mapping and index the documents")
    @ApiResponse(responseCode = "200", description = "Operation successful", content = { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Error", content = { @Content(mediaType = "application/json")})
    @GetMapping("/index_documents")
    public void indexDocuments(@RequestParam String filePath, @RequestParam(required = false) String ratingsPath) {
        try {
            try {
                //Remove existing index
                client.indices().delete(c -> c.index("films"));
            } catch(Exception e) {
                //in case the index does not exist, ignore and keep going
            }

            //Create the index for storing films
            client.indices().create(c -> c.index("films"));

            //Inserts the mapping for the films collection
            putFilmsMapping();

            //Creates the reader and set the batchSize
            ReadDataUtils rdu = new ReadDataUtils(filePath);

            new Thread(rdu::indexData).start();
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }

    /**
     * Puts the mapping for the field index
     */
    private void putFilmsMapping() {
        try {
            client.indices().putMapping(_0 -> _0
                    .index("films")
                    .properties("titleType", _1 -> _1
                            .keyword(_2 -> _2
                                    .boost(1.0)
                            )
                    )
                    .properties("primaryTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                                    .fields("raw", _3 -> _3
                                            .keyword(_4 -> _4
                                                    .boost(1.0)
                                            )
                                    )
                            )

                    )
                    .properties("originalTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                            )
                    )
                    .properties("isAdult", _1 -> _1
                            .boolean_(_2 -> _2
                                    .boost(1.0)

                            )
                    )
                    .properties("startYear", _1 -> _1
                            .integer(_2 -> _2
                                    .nullValue(0)
                            )
                    )
                    .properties("endYear", _1 -> _1
                            .integer(_2 -> _2
                                    .nullValue(0)
                            )
                    )
                    .properties("runtimeMinutes", _1 -> _1
                            .integer(_2 -> _2
                                    .nullValue(0)
                            )
                    )
                    .properties("genres", _1 -> _1
                            .keyword(_2 -> _2
                                    .boost(1.0)
                            )
                    )
            );
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }
}
