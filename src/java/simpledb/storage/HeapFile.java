package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    //数据文件
    private final File f;
    //文件结构
    private final TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
       // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        int pageNum = pid.getPageNumber();
        if(pageNum >= numPages()) {
            throw new IllegalArgumentException("the page does not exist in this page");
        }
        try {
            DataInputStream stream = new DataInputStream(new FileInputStream(f));
            stream.skipBytes(pageNum * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            int len = stream.read(data ,0, BufferPool.getPageSize());
            stream.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (IOException e) {
            System.err.println(e.toString());
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double)f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tId;

        private Iterator<Tuple> tupleIterator;

        private int tableId;

        private int pageNum;

        private int pageNo;

        public HeapFileIterator(TransactionId tId) {
            this.tId = tId;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            tableId = HeapFile.this.getId();
            pageNo = 0;
            pageNum = HeapFile.this.numPages();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(tableId, 0), Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null) {
                return false;
            }
            if(tupleIterator.hasNext()) {
                return true;
            } else if(++pageNo < pageNum){
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(tableId, pageNo), Permissions.READ_ONLY);
                tupleIterator = page.iterator();
                return tupleIterator.hasNext();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(hasNext()) {
                return tupleIterator.next();
            }
            throw new NoSuchElementException("No more Tuples");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageNo = 0;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(tableId, 0), Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }

        @Override
        public void close() {
            pageNo = 0;
            pageNum = 0;
            tupleIterator = null;
        }
    }
}

