package simpledb;

import java.io.*;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private static int privilage = 0;

    private int numPages = DEFAULT_PAGES;
    private static ConcurrentHashMap<PageId, Page> pid2page;
    private static ConcurrentHashMap<PageId, Integer> pid2pri;

    private static LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pid2page = new ConcurrentHashMap<>();
        pid2pri = new ConcurrentHashMap<>();

        lockManager = new LockManager(this);
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
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, an page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // if (pid.pageNumber() == 52) 
        //     System.out.println("getPage Thread:" + Thread.currentThread().getName() + Thread.currentThread().isInterrupted() + "\tAcquire " + tid + pid + ":" + perm);
        lockManager.acquire(tid, pid, perm);

        // Get Page
        Page page = pid2page.get(pid);
        if (page == null) {
            if (pid2page.size() >= numPages)
                evictPage();
            
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pid2page.put(pid, page);
        }
        
        pid2pri.put(pid, privilage++);
        return page;
    }

    /** Update Page related map before marked dirty */
    private void updateMap(TransactionId tid, Page page, Permissions perm)
            throws TransactionAbortedException, DbException {
        PageId pid = page.getId();
        if (!pid2page.containsKey(pid) && pid2page.size() >= numPages)
            evictPage();

        pid2page.put(pid, page);
        pid2pri.put(pid, privilage++);
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit)
            flushPages(tid);
        else { // this tid is aborted.
            // discard all dirty pages(if is read/write)
            ArrayList<LockManager.Lock> locks = lockManager.getLocksFromTid(tid);
            if (locks != null) {
                locks.stream()
                    .filter(lock -> lock.type == LockManager.Type.EXCLUSIVE)
                    .forEach(lock -> {discardPage(lock.pid);});
            }
        }

        lockManager.release(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will acquire
     * a write lock on the page the tuple is added to and any other pages that are
     * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
     * cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = file.insertTuple(tid, t);

        for (Page p : dirtyPages) {
            updateMap(tid, p, Permissions.READ_WRITE);
            p.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from and any other pages that are updated. May
     * block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);

        for (Page p : dirtyPages) {
            // getPage(tid, p.getId(), Permissions.READ_WRITE);
            updateMap(tid, p, Permissions.READ_WRITE);
            p.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
     * dirty data to disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (PageId pid : pid2page.keySet())
            flushPage(pid);
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages are removed from the
     * cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pid2pri.containsKey(pid)) {
            pid2pri.remove(pid);
            pid2page.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pid2page.get(pid);
        TransactionId dirtier = page.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, dirtier);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        ArrayList<LockManager.Lock> locks = lockManager.getLocksFromTid(tid);
        if (locks == null)
            return;

        for (LockManager.Lock l : locks)
            if (l.type == LockManager.Type.EXCLUSIVE && pid2page.containsKey(l.pid))
                flushPage(l.pid);
    }

    private static final EvictPolicy EVICTPOLICY = EvictPolicy.LRU;

    enum EvictPolicy {
        LRU, RANDOM, FIRST // RANDOM and FIRST may not support multi-threads
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId pid = null;
        switch (EVICTPOLICY) {
            case LRU:
                int minTime = Integer.MAX_VALUE;
                for (PageId p : pid2pri.keySet()) {
                    int t = pid2pri.get(p);
                    if (t < minTime && pid2page.get(p).isDirty() == null) {
                        pid = p;
                        minTime = t;
                    }
                }
                break;

            case RANDOM:
                PageId[] keys = (PageId[]) pid2page.keySet().toArray();
                pid = keys[(new Random()).nextInt(pid2page.size())]; // can not seed to 0
                break;
            case FIRST:
                for (PageId pid2 : pid2page.keySet())
                    if (pid2page.get(pid2).isDirty() == null) {
                        pid = pid2;
                        break;
                    }
                break;
        }
        if (pid == null)
            throw new DbException("No page is clean, can not evict.");

        try {
            flushPage(pid);
            discardPage(pid);
        } catch (IOException e) {
            throw new DbException(e.getMessage());
        }
    }

}