package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.search.exception.types.IndexAlreadyExistsException;
import co.empathy.academy.search.exception.types.IndexDoesNotExistException;
import co.empathy.academy.search.exception.types.InternalServerException;
import co.empathy.academy.search.utils.ElasticUtils;
import co.empathy.academy.search.utils.ReadDataUtils;
import co.empathy.academy.search.utils.TSVMerger;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

/**
 * Private controller used for creating indices and adding documents
 */
@Tag(name = "Private controller", description = "Allows to create and remove indexes. Also for indexing documents")
@RestController
@RequestMapping("/admin/api")
public class IndexController {
    private static final String INDEX = "films";
    private final ElasticsearchClient client = ElasticUtils.getClient();

    /**
     * Returns the name of the indices currently stored on the elastic container
     * @return Json with the names
     */
    @Operation(summary = "Returns a list with the current indices")
    @ApiResponse(responseCode = "200", description="Indices obtained", content = { @Content(mediaType= "application/json")})
    @ApiResponse(responseCode = "500", description="Internal server error", content = { @Content(mediaType= "application/json")})
    @GetMapping("/_cat/indices")
    public Map<String, IndexState> getIndices() throws InternalServerException {
        try {
            return client.indices().get(c -> c.index("*")).result();

        } catch(IOException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
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

    private void tryCreateIndex() {
        try {
            //Remove existing index
            client.indices().delete(c -> c.index(INDEX));
        } catch(Exception e) {
            //in case the index does not exist, ignore and keep going
        }
    }

    @PutMapping("/merge_documents")
    public void mergeDocuments(@RequestParam String filmsPath, @RequestParam String ratingsPath, @RequestParam String outputPath) {
        TSVMerger.mergeFiles(filmsPath, ratingsPath, outputPath);
    }

    /**
     * Removes and creates the films index for resetting purposes.
     * Then, id adds the mapping for the index
     * Finally, it indexes every document given on the file and ratings path
     */
    @Operation(summary = "Creates the field index, puts mapping and index the documents")
    @ApiResponse(responseCode = "200", description = "Operation successful", content = { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Error", content = { @Content(mediaType = "application/json")})
    @Parameter(name = "filmsPath", description = "Local file path of the films tsv", required = true)
    @Parameter(name = "filmsPath", description = "Local file path of the ratings tsv")
    @GetMapping("/index_documents")
    public void indexDocuments(@RequestParam String filmsPath) {
        try {
            tryCreateIndex();

            //Create the index for storing films
            client.indices().create(c -> c.index(INDEX));

            putSettings();

            //Inserts the mapping for the films collection
            putMapping();

            new Thread(() -> new ReadDataUtils().indexData(filmsPath)).start();

        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException("There was a problem processing your request", e);
        }
    }

    private void putSettings() throws IOException, ElasticsearchException {
        client.indices().close(_0 -> _0.index(INDEX));

        var customStandard = getClass().getClassLoader().getResourceAsStream("custom_standard_analyzer.json");

        client.indices().putSettings(_0 -> _0.withJson(customStandard));

        client.indices().open(_0 -> _0.index(INDEX));
    }

    /**
     * Puts the mapping for the field index
     */
    private void putMapping() throws IOException, ElasticsearchException {
        var mapping = getClass().getClassLoader().getResourceAsStream("mapping.json");

        client.indices().putMapping(_0 -> _0.index(INDEX).withJson(mapping));
    }
}
