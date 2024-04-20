package com.ruwaid.nosql.core;

import com.ruwaid.nosql.document.Document;
import com.ruwaid.nosql.index.Index;
import com.ruwaid.nosql.query.Query;
import com.ruwaid.nosql.schema.SchemaValidator;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Database {
    private SchemaValidator schemaValidator = new SchemaValidator();
    private static final String STORAGE_DIR = "data/db/";
    private final Map<String, Document> documents = new ConcurrentHashMap<>();
    private List<Document> documentsList = new ArrayList<>();
    private final Index index;
    private String databaseName;

    public Database(String name) {
        this.databaseName = name;
        this.index=new Index(databaseName, "default");

        // Ensure the database directory exists
        Path databasePath = Paths.get(STORAGE_DIR, databaseName);
        if (!Files.exists(databasePath)) {
            try {
                Files.createDirectories(databasePath);
            } catch (IOException e) {
                throw new RuntimeException("Could not create database directory", e);
            }
        }
    }

    public List<Document> find(Query query) {
        return documentsList.stream()
                .filter(query::matches)
                .collect(Collectors.toList());
    }

    // Include methods to add documents to the list for completeness
    public void addDocument(Document document) {
        if (this.documentsList == null) {
            this.documentsList = new ArrayList<>();
        }
        this.documentsList.add(document);
//        System.out.println("Document added with name: " + document.getData().get("name").asText());  // Debug statement
    }


    public List<Document> getAllDocuments(String collectionName) {
        try {
            Path collectionPath = Paths.get(STORAGE_DIR, databaseName, collectionName);
            return Files.walk(collectionPath)
                    .filter(Files::isRegularFile)
                    .map(path -> loadDocument(collectionName, path.getFileName().toString().replace(".json", "")))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve all documents", e);
        }
    }

    public void createCollection(String collectionName) {
        Path collectionPath = Paths.get(STORAGE_DIR, databaseName, collectionName);
        if (!Files.exists(collectionPath)) {
            try {
                Files.createDirectories(collectionPath);
            } catch (IOException e) {
                throw new RuntimeException("Could not create collection directory", e);
            }
        } else {
            System.out.println("Collection already exists.");
        }
    }

    public void deleteCollection(String collectionName) {
        Path collectionPath = Paths.get(STORAGE_DIR, databaseName, collectionName);
        try {
            Files.walk(collectionPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException("Could not delete collection", e);
        }
    }
    public Document createDocument(String collectionName, String json, String indexKey) {
        JSONObject jsonDocument = new JSONObject(json);
        schemaValidator.validate(jsonDocument);

        Document doc = Document.fromJson(json);
        documents.put(doc.getId(), doc);
        saveDocument(collectionName, doc);
        index.add(doc.getData().get(indexKey).asText(), doc.getId());  // Update index
        return doc;
    }

    public synchronized Document getDocument(String collectionName,String id) {
        Document doc = documents.get(id);
        if (doc == null) {
            // Try to load the document from the file system
            doc = loadDocument(collectionName,id);
            if (doc != null) {
                documents.put(id, doc); // Cache in memory
            }
        }
        return doc;
    }

    public synchronized boolean updateDocument(String collectionName, String id, String json, String indexKey, int expectedVersion) {
        Path path = Paths.get(STORAGE_DIR, databaseName, collectionName, id + ".json");
        if (Files.exists(path)) {
            try {
                Document currentDoc = documents.get(id);
                if (currentDoc == null) {
                    // If not in memory, load from disk
                    currentDoc = loadDocument(collectionName, id);
                }

                JSONObject jsonDocument = new JSONObject(json);
                schemaValidator.validate(jsonDocument);

                // Check if the current document's version matches the expected version
                if (currentDoc != null && currentDoc.getVersion() == expectedVersion) {
                    Document newDoc = Document.fromJson(json);

                    newDoc.incrementVersion();

                    Files.write(path, newDoc.toJson().getBytes());

                    documents.put(id, newDoc);

                    index.remove(currentDoc.getData().get(indexKey).asText(), id);  // Remove old index entry

                    index.add(newDoc.getData().get(indexKey).asText(), id);  // Add new index entry

                    return true;
                } else {
                    System.out.println("Update failed: version mismatch or document not found.");
                    return false;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to update document", e);
            }
        } else {
            System.out.println("Document not found in the file system, can't update.");
            return false;
        }
    }


    // Save document to the file system
    public void saveDocument(String collectionName, Document doc) {
        Path path = Paths.get(STORAGE_DIR, databaseName, collectionName, doc.getId() + ".json");
        try {
            Files.createDirectories(path.getParent());  // Ensure the directory exists
            Files.write(path, doc.toJson().getBytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save document", e);
        }
    }


    // Load document from the file system
    private Document loadDocument(String collectionName,String id) {
        Path path = Paths.get(STORAGE_DIR,databaseName,collectionName, id + ".json");
        try {
            String json = new String(Files.readAllBytes(path));
            return Document.fromJson(json);
        } catch (IOException e) {
            return null; // Document does not exist or could not be loaded
        }
    }

    // Delete document from memory and the file system
    public synchronized boolean deleteDocument(String collectionName, String id, String indexKey) {
        Document doc = documents.remove(id);
        if (doc != null) {
            index.remove(doc.getData().get(indexKey).asText(), id);  // Update index
            Path path = Paths.get(STORAGE_DIR, databaseName, collectionName, id + ".json");
            try {
                Files.deleteIfExists(path);
                return true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete document", e);
            }
        }
        return false;
    }

    // Initialize storage directory
    static {
        try {
            Path storagePath = Paths.get(STORAGE_DIR);
            if (!Files.exists(storagePath)) {
                Files.createDirectories(storagePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize storage directory", e);
        }
    }

}
