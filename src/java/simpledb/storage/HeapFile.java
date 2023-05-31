package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

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
    //页总数
    private int pageNum;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.pageNum = numPages();
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
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        int pageNo = pid.getPageNumber();
        if(pageNo >= pageNum) {
            throw new IllegalArgumentException("the page does not exist in this page");
        }
        try {
            DataInputStream stream = new DataInputStream(new FileInputStream(f));
            stream.skipBytes(pageNo * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            int len = stream.read(data ,0, BufferPool.getPageSize());
            stream.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageNo = page.getId().getPageNumber();
        DataOutputStream stream;
        if(pageNo == pageNum) {
            stream = new DataOutputStream(new FileOutputStream(f, true));
            stream.write(page.getPageData(), 0, BufferPool.getPageSize());
            stream.close();
            pageNum++;
        } else {
            byte[] data = new byte[(int) f.length()];
            DataInputStream in = new DataInputStream(new FileInputStream(f));
            int len = in.read(data);
            in.close();
            stream = new DataOutputStream(new FileOutputStream(f, false));
            stream.write(data, 0, BufferPool.getPageSize() * pageNo);
            stream.write(page.getPageData());
            int index = BufferPool.getPageSize() * (pageNo + 1);
            stream.write(data, index, data.length - index);
        }

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
        HeapPage page;
        for(int i = pageNum - 1; i >= 0; i--) {
            PageId pid = new HeapPageId(getId(), i);
            boolean holdsLock = Database.getBufferPool().holdsLock(tid, pid);
            page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if(page.getNumUnusedSlots() > 0) {
                page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                page.markDirty(true, tid);
                return Collections.singletonList(page);
            }
            if(!holdsLock) {
                Database.getBufferPool().unsafeReleasePage(tid, pid);
            }
        }
        byte[] data = HeapPage.createEmptyPageData();
        page = new HeapPage(new HeapPageId(getId(), numPages()), data);
        page.insertTuple(t);
      //  writePage(page);

        page.markDirty(true, tid);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if(t.getRecordId().getPageId().getPageNumber() >= pageNum) {
            throw new DbException("不存在该页");
        }
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
//        if(!tid.equals(page.isDirty())) {
//            throw new TransactionAbortedException();
//        }
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return Collections.singletonList(page);
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
                if(!tupleIterator.hasNext()) {
                    return hasNext();
                }
                return true;
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

