package com.ruwaid.nosql.query;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ruwaid.nosql.document.Document;
import org.json.JSONObject;

public class EqualityQuery implements Query {
    private String field;
    private Object value;

    public EqualityQuery(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public boolean matches(Document document) {
        ObjectNode jsonData = document.getData();
        boolean matches = jsonData.has(field) && jsonData.get(field).asText().equals(value);
//        System.out.println("Querying Document: Field=" + field + ", ExpectedValue=" + value + ", DocumentValue=" + jsonData.get(field).asText() + ", Matches=" + matches);  // Debug statement
        return matches;
    }

}