package com.azure.cosmos.examples.bulk.async;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

public class DbLockItem {
    public static final String STATUS_ABORTED = "ABORTED";
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_UNLOCKED = "UNLOCKED";
    private String id;
    private String db;
    private String token;
    private long dt; // date time of request
    private long lockDt; // date time of locking
    private long expDt; // date time of expiration
    private String status; // status: aborted, unlocked

    public DbLockItem() { }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public static DbLockItem requestLock(String db) {
        DbLockItem requestItem = new DbLockItem();
        requestItem.setDb(db);
        requestItem.setStatus(STATUS_ACTIVE);
        String token = UUID.randomUUID().toString();
        requestItem.setToken(token);
        requestItem.setId(token); // reuse token as id
        requestItem.setDt(System.currentTimeMillis());
        return requestItem;
    }

    private DbLockItem copy() {
        DbLockItem requestItem = new DbLockItem();
        requestItem.setDb(db);
        requestItem.setToken(token);
        requestItem.setDt(dt);
        requestItem.setExpDt(expDt);
        requestItem.setLockDt(lockDt);
        requestItem.setStatus(status);
        return requestItem;
    }

    public long getLockDt() {
        return lockDt;
    }

    public void setLockDt(long lockDt) {
        this.lockDt = lockDt;
    }

    private DbLockItem updateStatus(String newStatus) {
        DbLockItem abortItem = copy();
        abortItem.setStatus(newStatus);
        return abortItem;
    }

    public DbLockItem abort() {
        return updateStatus(STATUS_ABORTED);
    }

    public DbLockItem unlock() {
        return updateStatus(STATUS_UNLOCKED);
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public long getDt() {
        return dt;
    }

    public void setDt(long dt) {
        this.dt = dt;
    }

    public long getExpDt() {
        return expDt;
    }

    public void setExpDt(long expDt) {
        this.expDt = expDt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
