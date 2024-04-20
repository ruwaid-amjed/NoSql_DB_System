package com.ruwaid.nosql.core;

import com.ruwaid.nosql.document.Document;
import com.ruwaid.nosql.index.Index;

import java.util.*;
import java.util.stream.Collectors;

public class QueryProcessor {
    private Database database;
    private Index index;

    public QueryProcessor(Database database, Index index) {
        this.database = database;
        this.index = index;
    }

    public List<Document> findByProperty(String collectionName, String propertyName, String propertyValue) {
        Set<String> documentIds = index.getDocumentIds(propertyValue);

        // If the index contains the property value, retrieve documents directly
        if (!documentIds.isEmpty()) {
            return documentIds.stream()
                    .map(id -> database.getDocument(collectionName, id))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } else {
            // If not indexed, fall back to full scan (less efficient)
            return database.getAllDocuments(collectionName).stream()
                    .filter(doc -> propertyValue.equals(doc.getData().get(propertyName).asText()))
                    .collect(Collectors.toList());
        }
    }
}
