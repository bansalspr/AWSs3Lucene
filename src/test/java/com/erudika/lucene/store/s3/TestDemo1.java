package com.erudika.lucene.store.s3;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;

public class TestDemo1 {
//    protected Analyzer analyzer = new StandardAnalyzer();
    protected Analyzer analyzer = new SimpleAnalyzer();

    @BeforeClass
    public static void initDatabase() throws IOException {

    }

    @AfterClass
    public static void closeDatabase() {
    }

    @Before
    public void initAttributes() throws Exception {
    }

    protected void addDocuments(final Directory directory, final IndexWriterConfig.OpenMode openMode, final boolean useCompoundFile) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(openMode);
        config.setUseCompoundFile(useCompoundFile);

        final String text1 = "sprinklr started in september";
        final String text2 = "sprinklr has two office in india";
        final String text3 = "many students doing intern in sprinklr company";
        final String text4 = "company in india";

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 1; i <= 100; i++) {
                final Document doc = new Document();
                if (i <= 200) {
                    doc.add(new Field("odd_field", text1, TextField.TYPE_NOT_STORED));
                    doc.add(new Field("even_field", text2, TextField.TYPE_NOT_STORED));
                }
                if (i <= 400) {
                    doc.add(new Field("odd_field", text3, TextField.TYPE_NOT_STORED));
                }
                if (i > 800) {
                    doc.add(new Field("odd_field", text1, TextField.TYPE_NOT_STORED));
                }
                if (i > 600) {
                    doc.add(new Field("even_field", text4, TextField.TYPE_NOT_STORED));
                }
                if (i > 200 && i <= 800) {
                    doc.add(new Field("even_field", text4, TextField.TYPE_NOT_STORED));
                }
                writer.addDocument(doc);
            }

        }

    }

    protected void queryDocument(final Directory directory) throws IOException {

        try (DirectoryReader ireader = DirectoryReader.open(directory)) {
            final IndexSearcher isearcher = new IndexSearcher(ireader);
//			System.out.println("\n=========QUERY=========");
            Query query = new TermQuery(new Term("odd_field", "students"));
            final ScoreDoc[] hits = isearcher.search(query, 10000).scoreDocs;
            System.out.println("Number of hits: "+ hits.length);
        }
    }
}
