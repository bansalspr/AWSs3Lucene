/*
 * Copyright 2004-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.erudika.lucene.store.s3;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Collection;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author kimchy
 */
public class S3DirectoryBenchmarkITest extends AbstractS3DirectoryITest {

	private static Directory fsDirectory;
	private static Directory ramDirectory;
	private static Directory s3Directory;

	private final Collection<String> docs = loadDocuments(50, 5);
	private final OpenMode openMode = OpenMode.CREATE;
	private final boolean useCompoundFile = true;

	@BeforeClass
	public static void setUp() throws Exception {
		String bucketname = "example4-xyz";
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
		addDocuments(dir, openMode, useCompoundFile, docs);
		final long stop = System.currentTimeMillis();
		System.out.println("WritingTime: "+(stop-start)+" ms");
		final long start2 = System.currentTimeMillis();
		queryDocument(dir);
		final long stop2 = System.currentTimeMillis();
		System.out.println("QueryTime: "+(stop2-start2)+" ms");
		return stop2 - start;
	}

}
