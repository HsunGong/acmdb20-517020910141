package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");) {
            int pos = pid.pageNumber() * BufferPool.getPageSize();
            randomAccessFile.seek(pos);
            randomAccessFile.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e.getCause());
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return ((int) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator{
        private int curPid;
        private Iterator<Tuple> tupleIterator;
        private TransactionId transactionId;

        public HeapFileIterator(TransactionId tid){ this.transactionId = tid; }

        @Override
        public void open() throws DbException, TransactionAbortedException{
            this.curPid = 0;

            PageId pageId = new HeapPageId(getId(), curPid);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId, Permissions.READ_ONLY);
            this.tupleIterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            // if closed
            if (this.tupleIterator == null) return false;
            // has next tuple or next page
            if (this.tupleIterator.hasNext() || this.curPid < numPages() - 1) return true;
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException{
            if (!this.hasNext()) throw new NoSuchElementException("no more ele");
            if (this.tupleIterator.hasNext()) return tupleIterator.next();
            
            this.curPid += 1;

            PageId pageId = new HeapPageId(getId(), curPid);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
            tupleIterator = page.iterator();
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }

        @Override
        public void close(){
            this.curPid = 0;
            this.tupleIterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

