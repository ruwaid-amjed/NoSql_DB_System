package nosql.core;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ruwaid.nosql.core.Database;
import com.ruwaid.nosql.document.Document;
import com.ruwaid.nosql.query.EqualityQuery;
import com.ruwaid.nosql.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseTest {
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database("TestDatabase");
        JsonNodeFactory factory = JsonNodeFactory.instance;

        // Creating ObjectNode directly
        ObjectNode jsonJohn = factory.objectNode();
        jsonJohn.put("name", "John");
        jsonJohn.put("age", 30);
        database.addDocument(new Document(jsonJohn));

        ObjectNode jsonJane = factory.objectNode();
        jsonJane.put("name", "Jane");
        jsonJane.put("age", 25);
        database.addDocument(new Document(jsonJane));
    }

    @Test
    void testFindByName() {
        Query query = new EqualityQuery("name", "John");
        List<Document> results = database.find(query);
        assertEquals(1, results.size());
        assertEquals("John", results.get(0).getData().get("name").asText());
    }
}
