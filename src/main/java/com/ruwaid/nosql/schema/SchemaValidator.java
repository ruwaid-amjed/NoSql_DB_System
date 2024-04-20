package com.ruwaid.nosql.schema;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.io.InputStream;

public class SchemaValidator {
    private Schema schema;

    public SchemaValidator() {
        try (InputStream inputStream = getClass().getResourceAsStream("/document-schema.json")) {
            JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
            this.schema = SchemaLoader.load(rawSchema);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load schema", e);
        }
    }

    public void validate(JSONObject jsonDocument) {
        schema.validate(jsonDocument);  // Throws ValidationException if not valid
    }

}
