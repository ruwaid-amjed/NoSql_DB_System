package com.ruwaid.nosql.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.UUID;

public class Document {
    private static final ObjectMapper MAPPER=new ObjectMapper();

    private String id;
    private ObjectNode data;
    private int version;  // Version field for optimistic locking

    public Document(ObjectNode data){
        this.id=UUID.randomUUID().toString();// Assign a unique ID
        this.data=data;
        this.version=0;
    }

    public String getId() {
        return id;
    }

    public ObjectNode getData() {
        return data;
    }

    public void setData(ObjectNode data) {
        this.data = data;
    }

    // Static method to create a Document from a JSON string
    public static Document fromJson(String json){
        try{
            ObjectNode data=(ObjectNode) MAPPER.readTree(json);

            return new Document(data);
        }catch (Exception e){
            throw new RuntimeException("Invalid JSON data", e);
        }
    }

    // Convert the Document data to a JSON string
    public String toJson() {
        return data.toString();
    }

    // Add or update a field in the JSON document
    public void put(String fieldName, JsonNode value) {
        data.set(fieldName, value);
    }

    // Remove a field from the JSON document
    public void remove(String fieldName) {
        data.remove(fieldName);
    }

    // Retrieve a field from the JSON document
    public JsonNode get(String fieldName) {
        return data.get(fieldName);
    }


    public int getVersion() {
        return this.version;
    }

    public void incrementVersion() {
        this.version++;
    }
}
