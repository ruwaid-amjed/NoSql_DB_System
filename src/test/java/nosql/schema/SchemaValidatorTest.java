package nosql.schema;

import com.ruwaid.nosql.schema.SchemaValidator;
import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaValidatorTest {
    private SchemaValidator validator;

    @BeforeEach
    void setUp() {
        validator = new SchemaValidator();
    }

    @Test
    void testValidDocument() {
        String validJson = "{\"name\":\"John Doe\", \"age\":30, \"email\":\"john.doe@example.com\"}";
        JSONObject jsonObject = new JSONObject(validJson);
        assertDoesNotThrow(() -> validator.validate(jsonObject));
    }

    @Test
    void testInvalidDocumentMissingName() {
        String invalidJson = "{\"age\":30, \"email\":\"john.doe@example.com\"}";
        JSONObject jsonObject = new JSONObject(invalidJson);
        assertThrows(ValidationException.class, () -> validator.validate(jsonObject));
    }

    @Test
    void testInvalidDocumentExtraField() {
        String invalidJson = "{\"name\":\"John Doe\", \"age\":30, \"email\":\"john.doe@example.com\", \"extra\":\"data\"}";
        JSONObject jsonObject = new JSONObject(invalidJson);
        assertThrows(ValidationException.class, () -> validator.validate(jsonObject));
    }
}
