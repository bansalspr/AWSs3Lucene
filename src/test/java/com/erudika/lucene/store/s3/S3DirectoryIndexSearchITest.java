package com.erudika.lucene.store.s3;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class S3DirectoryIndexSearchITest extends AbstractS3DirectoryITest {

	private Directory directory;

	@Before
	public void setUp() throws Exception {
		String bucket1 = "example4-xyz";
        directory = new S3Directory(bucket1);
        ((S3Directory) directory).create();
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("==END");
        directory.close();
        ((S3Directory) directory).delete();
	}

	@Test
	public void testSearch() throws IOException, ParseException {

		try (IndexWriter iwriter = new IndexWriter(directory, getIndexWriterConfig())) {
			final Document doc = new Document();
			final String text = "This is the text to be indexed.";
			doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
			iwriter.addDocument(doc);
			if (iwriter.hasUncommittedChanges()) {
				iwriter.commit();
			}	if (iwriter.isOpen()) {
				iwriter.getDirectory().close();
			}	iwriter.forceMerge(1, true);
		}
		// Now search the index:
		try (DirectoryReader ireader = DirectoryReader.open(directory)) {
			final IndexSearcher isearcher = new IndexSearcher(ireader);
			// Parse a simple query that searches for "text":

			final QueryParser parser = new QueryParser("fieldname", analyzer);
			final Query query = parser.parse("text");
			final ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
			Assert.assertEquals(1, hits.length);
			// Iterate through the results:
			for (final ScoreDoc hit : hits) {
				final Document hitDoc = isearcher.doc(hit.doc);
				Assert.assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
			}
			ireader.close();
		} catch (final ParseException e) {
			throw new IOException(e);
		}

	}

	private IndexWriterConfig getIndexWriterConfig() {
		final IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
//		config.setUseCompoundFile(false);
		return config;
	}
}
