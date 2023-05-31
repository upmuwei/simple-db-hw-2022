package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    //最大page数目
    private int maxPageNum;
    //准备置换出缓存的页
    private int evictPageNum;
    private  List<Page> cachedPages;
    private Map<PageId, Page> idPageMap;

    //事务加锁页对应关系
   // private ConcurrentMap<TransactionId, List<PageId>> transactionIdConcurrentMap;
    //false为共享锁，true为排它锁
 //   private ConcurrentMap<PageId, Boolean> lockConcurrentMap;
    //在页上锁的数目
 //   private ConcurrentMap<PageId, Integer> lockNumConcurrentMap;
    private final LockManager lockManager = new LockManager();
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxPageNum = numPages;
        cachedPages = new ArrayList<>(numPages);
        idPageMap = new HashMap<>();
        evictPageNum = 0;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if(perm == Permissions.READ_WRITE) {
            lockManager.getWriteLock(tid, pid);
        } else if(perm == Permissions.READ_ONLY){
            lockManager.getReadLock(tid, pid);
        }
        Page page = idPageMap.get(pid);
        if(page != null) {
            return page;
        }
        if(cachedPages.size() == maxPageNum) {
            evictPage();
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        page = file.readPage(pid);
        cachedPages.add(page);
        idPageMap.put(pid, page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        lockManager.releaseAllLock(tid);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if(commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for(PageId pageId : lockManager.getLockPageId(tid)) {
                if(idPageMap.get(pageId) != null && tid.equals(idPageMap.get(pageId).isDirty())) {
                    removePage(pageId);
                }
            }
        }
        transactionComplete(tid);
    }

    
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> rPages = file.insertTuple(tid, t);
        for(Page rPage : rPages) {
            if(idPageMap.get(rPage.getId()) == null) {
                cachedPages.add(rPage);
                idPageMap.put(rPage.getId(), rPage);
                lockManager.getWriteLock(tid, rPage.getId());
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        file.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page page : cachedPages) {
            if(page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        idPageMap.remove(pid);
        for(int i = 0; i < cachedPages.size(); i++) {
            if(cachedPages.get(i).getId().equals(pid)) {
                cachedPages.remove(i);
                break;
            }
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = idPageMap.get(pid);
        if(page != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Page cachedPage : cachedPages) {
            if (tid.equals(cachedPage.isDirty())) {
                flushPage(cachedPage.getId());
                cachedPage.markDirty(false, tid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        evictPageFIFO();
    }

    private void evictPageFIFO() throws DbException {
        Page page = null;// = cachedPages.get(0).getId();
        int signPageNum = 0;
        while(signPageNum != maxPageNum) {
            page = cachedPages.get(signPageNum);
            if(page.isDirty() == null) {
                break;
            }
            signPageNum++;
        }
        if(signPageNum == maxPageNum) {
            throw new DbException("没有页可以被置换");
        } else {
            idPageMap.remove(page.getId());
            cachedPages.remove(evictPageNum);
        }
    }
}
