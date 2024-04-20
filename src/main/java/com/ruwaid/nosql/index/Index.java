package com.ruwaid.nosql.index;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.*;

public class Index {
    private Map<String, Set<String>> indexMap = new HashMap<>();
    private Path indexPath;

    public Index(String databaseName, String indexName) {
        try {
            String indexDir = "data/db/" + databaseName + "/_indexes";
            Files.createDirectories(Paths.get(indexDir));
            indexPath = Paths.get(indexDir, indexName + ".idx");
            loadIndex();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize index", e);
        }
    }

    public void add(String key, String documentId) {
        indexMap.computeIfAbsent(key, k -> new HashSet<>()).add(documentId);
        saveIndex();
    }

    public void remove(String key, String documentId) {
        if (indexMap.containsKey(key)) {
            indexMap.get(key).remove(documentId);
            if (indexMap.get(key).isEmpty()) {
                indexMap.remove(key);
            }
            saveIndex();
        }
    }

    public Set<String> getDocumentIds(String key) {
        return indexMap.getOrDefault(key, Collections.emptySet());
    }

    private void saveIndex() {
        try {
            List<String> lines = new ArrayList<>();
            for (Map.Entry<String, Set<String>> entry : indexMap.entrySet()) {
                for (String id : entry.getValue()) {
                    lines.add(entry.getKey() + "=" + id);
                }
            }
            Files.write(indexPath, lines);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save index", e);
        }
    }

    private void loadIndex() {
        if (Files.exists(indexPath)) {
            try {
                List<String> lines = Files.readAllLines(indexPath);
                indexMap.clear();
                for (String line : lines) {
                    String[] parts = line.split("=");
                    this.add(parts[0], parts[1]);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load index", e);
            }
        }
    }
}
