


import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class IndexTester {

    private static Path path = FileSystems.getDefault().getPath("C:\\temp\\tester");

    private static Analyzer analyzer = new StandardAnalyzer();
    
    final static Logger log = LogManager.getLogger(IndexTester.class);


    public static void main(String args[]) throws IOException, ParseException {
        Directory idx = FSDirectory.open(path);
        index("C:\\temp\\test_index");

        Term term = new Term("Doc", "quart?"); // must be lower case.

        WildcardQuery wc = new WildcardQuery(term);

        SpanQuery spanTerm = new SpanMultiTermQueryWrapper<WildcardQuery>(wc);
        IndexReader indexReader = DirectoryReader.open(idx);
        
        System.out.println("Term freq=" + indexReader.totalTermFreq(term));
        System.out.println("Term freq=" + indexReader.getSumTotalTermFreq("Doc"));

        IndexSearcher isearcher = new IndexSearcher(indexReader);
         TopDocs docs = isearcher.search(spanTerm, 1);
        System.out.println("totalHits = " + docs.totalHits);
        IndexReaderContext indexReaderContext = isearcher.getTopReaderContext();
        TermContext context = TermContext.build(indexReaderContext, term);
        TermStatistics termStatistics = isearcher.termStatistics(term, context);
        System.out.println("termStatics=" + termStatistics.totalTermFreq());
    }

    public static List<String> query(Query query, MutableLong totalHits, MutableLong totalDocs)
            throws IOException {

        List<String> files = new Vector<String>();
        Directory idx = FSDirectory.open(path);
        DirectoryReader indexReader = DirectoryReader.open(idx);
        IndexSearcher isearcher = new IndexSearcher(indexReader);


        TopDocs topDocs = isearcher.search(query, 100);

        ScoreDoc[] top = topDocs.scoreDocs;

        System.out.println(topDocs.totalHits);
        totalHits.setValue(topDocs.totalHits);
        totalDocs.setValue(top.length);

        log.trace("top length" + top.length);
        for (int i = 0; i < top.length; i++) {
            int docID = top[i].doc;
            Document doc = isearcher.doc(docID);
            Path path = Paths.get(doc.get("Path"));
            String fileName = path.getFileName().toString();
            log.trace("match fileName =" + fileName);
            files.add(fileName);
        }
        
        indexReader.close();
        idx.close();
        return files;
    }

    public static void index(String dir) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setCommitOnClose(true);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        Directory idx = FSDirectory.open(path);
        IndexWriter indexWriter = new IndexWriter(idx, config);
        List<File> files = lgDir(dir);
        for (File f : files) {
            log.trace("filename=" + f.getName());
            //boolean indexExists = doesIndexExist();
           // log.trace("indexExists" + indexExists);
            addDoc(indexWriter, f);       
        }
        indexWriter.commit();
        indexWriter.close();
        idx.close();

    }



    /**
     * Add the document to the index.
     * 
     * @param writer
     * @param filePath
     * @throws IOException
     */
    private static void addDoc(IndexWriter writer, File filePath) throws IOException {
        Document doc = new Document();

        //byte[] encoded = Files.readAllBytes(Paths.get(filePath.getCanonicalPath()));
        List<String> lines = Files.readAllLines(Paths.get(filePath.getCanonicalPath()), Charset.forName("Cp1252"));
        StringBuffer buf = new StringBuffer();
       java.util.Iterator<String> iter =lines.iterator();
       while (iter.hasNext()) {
           buf.append(iter.next());
           buf.append("\n");
       }
        //String content = new String(encoded, "UTF-8");
        //if (content.length() > 0) {
            log.trace(filePath.getCanonicalPath());
            doc.add(new StringField("Path", filePath.getCanonicalPath(), Field.Store.YES));
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(filePath.lastModified());
            doc.add(new LongField("Date", filePath.lastModified(), Field.Store.YES));
            // log.trace("content size" + content.length());
            doc.add(new TextField("Doc", buf.toString(), Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
       // }
    }

    public static List<File> lgDir(String directory) {

        File d = new File(directory);
        File[] f = d.listFiles();
        List<File> myList = new Vector<File>();
        for (File f1 : f) {
            myList.add(f1);
        }

        log.trace("size count of myList =" + myList.size());
        return myList;
    }


}
