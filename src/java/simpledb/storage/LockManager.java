package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    private  ReentrantReadWriteLock.ReadLock readLock;
    private  ReentrantReadWriteLock.WriteLock writeLock;

    //事务加锁页对应关系
    private ConcurrentMap<TransactionId, List<PageId>> transactionIdConcurrentMap;
    //false为共享锁，true为排它锁
    private ConcurrentMap<PageId, Boolean> lockConcurrentMap;
    //在页上锁的数目
    private ConcurrentMap<PageId, Integer> lockNumConcurrentMap;

    public LockManager() {
        ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
        readLock = reentrantLock.readLock();
        writeLock = reentrantLock.writeLock();
        transactionIdConcurrentMap = new ConcurrentHashMap<>();
        lockConcurrentMap = new ConcurrentHashMap<>();
        lockNumConcurrentMap = new ConcurrentHashMap<>();
    }
    public boolean getReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if(holdsLock(tid, pid)) {
            return true;
        }
        int  waitTime = 0;
        while(lockConcurrentMap.get(pid)!= null && lockConcurrentMap.get(pid)) {
            if(waitTime > 30) {
                releaseAllLock(tid);
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            waitTime++;
        }
        writeLock.lock();
        int num = lockNumConcurrentMap.get(pid) != null ? lockNumConcurrentMap.get(pid) : 0;
        lockNumConcurrentMap.put(pid, num + 1);
        lockConcurrentMap.putIfAbsent(pid, false);
        List<PageId> list = transactionIdConcurrentMap.computeIfAbsent(tid, k -> new ArrayList<>());
        list.add(pid);
        writeLock.unlock();
        return true;
    }

    public  boolean getWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if(holdsLock(tid, pid)) {
            return lockUpgrade(tid, pid);
        }
        int waitTime = 0;
        while(lockNumConcurrentMap.get(pid) != null && lockNumConcurrentMap.get(pid) > 0) {
            if(waitTime > 30) {
                releaseAllLock(tid);
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            waitTime++;
        }
        //   lockManager.getWriteLock();
        lockNumConcurrentMap.put(pid, 1);
        lockConcurrentMap.put(pid, true);
        List<PageId> list = transactionIdConcurrentMap.computeIfAbsent(tid, k -> new ArrayList<>());
        list.add(pid);

        //    lockManager.releaseWriteLock();
        return true;
    }

    private  boolean lockUpgrade(TransactionId tid, PageId pid) throws TransactionAbortedException {
        int waitTime = 0;
        while(lockNumConcurrentMap.get(pid) != 1) {
            if(waitTime > 30) {
                releaseAllLock(tid);
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(10);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            waitTime++;
        }
        lockConcurrentMap.replace(pid, true);
        return true;
    }

    public  void releaseLock(TransactionId tid, PageId pid) {
        List<PageId> list = transactionIdConcurrentMap.get(tid);
        if(list == null) {
            return;
        }
        list.remove(pid);
        writeLock.lock();
        int num = lockNumConcurrentMap.get(pid);
        if(num == 0) {
            writeLock.unlock();
            return;
        }
        if(num == 1) {
            lockNumConcurrentMap.remove(pid);
            lockConcurrentMap.remove(pid);
        }else {
            lockNumConcurrentMap.replace(pid, num - 1);
        }
        writeLock.unlock();
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        List<PageId> list = transactionIdConcurrentMap.get(tid);
        if(list != null) {
            for(PageId pageId : list) {
                if(pageId.equals(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void releaseAllLock(TransactionId tid) {
        List<PageId> pageIds = transactionIdConcurrentMap.get(tid);
        if(pageIds == null) {
            return;
        }
        writeLock.lock();
        for(PageId pid : pageIds) {
            int num = lockNumConcurrentMap.get(pid);
            if(num == 1) {
                lockNumConcurrentMap.remove(pid);
                lockConcurrentMap.remove(pid);
            }else {
                lockNumConcurrentMap.replace(pid, num - 1);
            }
        }
        writeLock.unlock();
        pageIds.clear();
    }

    public List<PageId> getLockPageId(TransactionId tid) {
        return transactionIdConcurrentMap.get(tid);
    }
}
