package com.ruwaid.nosql.query;

import com.ruwaid.nosql.document.Document;

public interface Query {
    boolean matches(Document document);
}
