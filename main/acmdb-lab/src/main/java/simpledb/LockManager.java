package simpledb;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    enum Type {
        EXCLUSIVE, SHARED
    }
    public class Lock {
        public TransactionId tid;
        public PageId pid;
        public Type type;

        Lock(TransactionId transactionId, PageId pageId, Type t) {
            tid = transactionId;
            pid = pageId;
            type = t;
        }

        public String toString() {
            return "(" + tid + " " + pid.pageNumber() + " " + type + ")";
        }
    }

    private ConcurrentHashMap<TransactionId, ArrayList<Lock>> tid2locks;
    private ConcurrentHashMap<PageId, ArrayList<Lock>> pid2exc;
    private ConcurrentHashMap<PageId, ArrayList<Lock>> pid2sha;
    private Random rand = new Random(0);
    private Object owner;

    public LockManager(Object owner) {
        tid2locks = new ConcurrentHashMap<>();
        pid2exc = new ConcurrentHashMap<>();
        pid2sha = new ConcurrentHashMap<>();
        this.owner = owner;
    }

    public ArrayList<Lock> getLocksFromTid(TransactionId tid) {
        return tid2locks.get(tid);
    }

    private static final int WAIT_TIME = 50;
    private static final int ABORT_MIN_TIME = 200;
    private static final int ABORT_MAX_TIME = 400; // abort time in [Base, Base + Var] millisecond

    private void waitAbort(long st, String msg) throws TransactionAbortedException {
        long et = System.currentTimeMillis();
        // System.out.println(et - st + ":" + msg);
        if (et - st > ABORT_MIN_TIME + rand.nextInt(ABORT_MAX_TIME - ABORT_MIN_TIME)){
            // System.err.println(msg + ":" + (et - st));
            throw new TransactionAbortedException(msg);
        }

        try {
            owner.wait(WAIT_TIME);
            // System.out.println("wait1 end");
        } catch (InterruptedException e) {
        }
    }

    public void acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        // if (pid.pageNumber() == 52)
        //     System.out.println("Thread:" + Thread.currentThread().getName() + Thread.currentThread().isInterrupted() + "\tAcquire " + tid + pid + ":" + perm);
        Lock lock;
        if (!pid2exc.containsKey(pid))
            pid2exc.put(pid, new ArrayList<>());
        if (!pid2sha.containsKey(pid))
            pid2sha.put(pid, new ArrayList<>());
        if (!tid2locks.containsKey(tid))
            tid2locks.put(tid, new ArrayList<>());

        long st = System.currentTimeMillis(); // Start time
        if (perm == Permissions.READ_ONLY) {
            while (!pid2exc.get(pid).isEmpty()) {
                if (pid2exc.get(pid).get(0).tid == tid)
                    break;
                
                // System.out.println(pid2exc.get(pid).get(0));
                waitAbort(st, "RO: " + tid + "," + pid + " aborted");
            }
            lock = new Lock(tid, pid, Type.SHARED);
            // if (pid.pageNumber() == 52)
            //     System.out.println("Acquired(RO) " + tid + pid);
 
            pid2sha.putIfAbsent(pid, new ArrayList<>());
            pid2sha.get(pid).add(lock);
        } else {
            while (!pid2exc.get(pid).isEmpty() || !pid2sha.get(pid).isEmpty()) {
                if (pid2exc.get(pid).isEmpty()) {
                    boolean single = true;
                    for (Lock l : pid2sha.get(pid)) {
                        if (l.tid != tid) {
                            single = false;
                            break;
                        }
                    }
                    if (single)
                        break;
                } else if (pid2exc.get(pid).get(0).tid == tid)
                    break;

                    waitAbort(st, "RW: " + tid + "," + pid + " aborted");
                    // System.out.println("Thread: " + Thread.currentThread().getName() + Thread.currentThread().isInterrupted() + tid + pid);
            }
            lock = new Lock(tid, pid, Type.EXCLUSIVE);
            // if (pid.pageNumber() == 52)
            //     System.out.println("Acquired(RW) " + tid + pid);
            pid2exc.putIfAbsent(pid, new ArrayList<>());
            pid2exc.get(pid).add(lock);
        }
        tid2locks.get(tid).add(lock);
    }

    public void release(TransactionId tid, PageId pid) {
        // if (pid.pageNumber() == 52)
        //     System.out.println("Thread:" + Thread.currentThread().getName() + Thread.currentThread().isInterrupted() + "\tRelease " + tid + pid);
        if (tid2locks.containsKey(tid)) 
            tid2locks.get(tid).removeIf(lock -> lock.pid == pid);
        
        if (pid2exc.containsKey(pid))
            pid2exc.get(pid).removeIf(lock -> lock.tid == tid);
        if (pid2sha.containsKey(pid))
            pid2sha.get(pid).removeIf(lock -> lock.tid == tid);
        
        owner.notifyAll();
    }

    public void release(TransactionId tid) {
        if (tid2locks.containsKey(tid)) {
            ArrayList<Lock> locks = tid2locks.get(tid);
            for (Lock l : locks) {
                PageId pageId = l.pid;
                // if (pageId.pageNumber() == 52)
                //     System.out.println("Thread:" + Thread.currentThread().getName() + Thread.currentThread().isInterrupted() + "\tRelease2 " + tid + pageId);
                pid2exc.get(pageId).removeIf(lock -> lock.tid == tid);
                pid2sha.get(pageId).removeIf(lock -> lock.tid == tid);
            }
            locks.clear();

            tid2locks.remove(tid);
        }

        owner.notifyAll();
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        // if (pid.pageNumber() == 52)
        //     System.out.println("Hold " + tid + pid);
        for (Lock lock : tid2locks.get(tid))
            if (lock.pid == pid) return true;
        return false;
    }

}