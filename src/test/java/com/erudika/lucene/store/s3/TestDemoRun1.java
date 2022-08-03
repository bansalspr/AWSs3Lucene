package com.erudika.lucene.store.s3;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Collection;

public class TestDemoRun1 extends TestDemo1{
    private static Directory fsDirectory;
    private static Directory ramDirectory;
    private static Directory s3Directory;

    private final IndexWriterConfig.OpenMode openMode = IndexWriterConfig.OpenMode.CREATE;
    private final boolean useCompoundFile = true;

    @BeforeClass
    public static void setUp() throws Exception {
        String bucketname = "example5-xyz";
        s3Directory = new S3Directory(bucketname);
        ((S3Directory) s3Directory).create();
        ramDirectory = new MMapDirectory(FileSystems.getDefault().getPath("src/test/resources"));
        fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath("src/test/resources"));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("-------CLOSING----------");
        s3Directory.close();
        ((S3Directory) s3Directory).delete();
    }

    @Test
    public void testTiming() throws IOException {
        System.out.println("====RAMDirectory Time====");
        final long ramTiming = timeIndexWriter(ramDirectory);
        System.out.println("RAMDirectory Time: " + ramTiming + " ms\n");

        System.out.println("====FSDirectory Time====");
        final long fsTiming = timeIndexWriter(fsDirectory);
        System.out.println("FSDirectory Time : " + fsTiming + " ms\n");

        System.out.println("====S3Directory Time====");
        final long s3Timing = timeIndexWriter(s3Directory);
        System.out.println("S3Directory Time : " + s3Timing + " ms\n");
    }

    private long timeIndexWriter(final Directory dir) throws IOException {
        final long start = System.currentTimeMillis();
        addDocuments(dir, openMode, useCompoundFile);
        final long stop = System.currentTimeMillis();
        System.out.println("WritingTime: "+(stop-start)+" ms");
        final long start2 = System.currentTimeMillis();
        queryDocument(dir);
        final long stop2 = System.currentTimeMillis();
        System.out.println("QueryTime: "+(stop2-start2)+" ms");
        return stop2 - start;
    }
}
