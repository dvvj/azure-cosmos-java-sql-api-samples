package com.azure.cosmos.examples.bulk.async;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.examples.common.Child;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DbLockHelper {
    private static final Gson GSON = new Gson();
    private static int SLEEP_INTERVAL = 5000;
    private static int RETRY_BACKOFF_INTERVAL = 10000;
    private static Logger logger = LoggerFactory.getLogger(DbLockHelper.class);

    public static boolean releaseLock(CosmosAsyncContainer container, DbLockItem currLock) throws Exception {
        if (checkIfLocked(container, currLock.getDb(), currLock.getToken())) {
            String msg = "Lock state error: expecting to be locked by us only!";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        return updateStatus(container, currLock, DbLockItem.STATUS_UNLOCKED);
    }

    // demonstrates set operation. it is same as add except for array.
    private static boolean updateStatus(CosmosAsyncContainer container, DbLockItem currLock, String newStatus) {
        logger.info("Executing Patch with 'set' operations");

        CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
        // does not exist, will be added (same behavior as add)
        cosmosPatchOperations.set("/status", newStatus);

        CosmosPatchItemRequestOptions options = new CosmosPatchItemRequestOptions();
        try {
            CosmosItemResponse<DbLockItem> response = container.patchItem(
                    currLock.getId(), new PartitionKey(currLock.getDb()),
                    cosmosPatchOperations, options, DbLockItem.class).block();
            DbLockItem respItem = response.getItem();
            logger.info("Item with ID {} has been patched: status = {}", respItem.getId(), respItem.getStatus());
            return true;
        } catch (Exception e) {
            logger.error("failed", e);
            return false;
        }
    }

    private static void cancelAllLocks(CosmosAsyncContainer container, String db) {
        List<DbLockItem> locks = DbLockHelper.queryLocks(container, db);
        locks.forEach(l -> updateStatus(container, l, DbLockItem.STATUS_ABORTED));
    }

    public static boolean dbgDeleteLockItem(CosmosAsyncContainer container, DbLockItem itemToDelete) {
        container.deleteItem(itemToDelete.getId(), new PartitionKey(itemToDelete.getDb())).block();
        return true;
    }

    public static DbLockItem requestLock(CosmosAsyncContainer container, String db) throws Exception {
        DbLockItem lockItem = DbLockItem.requestLock(db);
        logger.warn("Checking if locked");
        while (checkIfLocked(container, db)) {
            Thread.sleep(SLEEP_INTERVAL);
        }
        logger.warn("Trying lock db");
        lockItem.setLockDt(System.currentTimeMillis());
        container.createItem(lockItem).block();
        if (checkIfLocked(container, lockItem.getDb(), lockItem.getToken())) {
            // todo: rare case when it's also locked by others!!
            // delete all locks and retry
            logger.warn("Conflict detected: cancelling all active locks");
            cancelAllLocks(container, db);
            int backoffInterval = new Random().nextInt(RETRY_BACKOFF_INTERVAL) + 2000; // 2 - 12 sec
            logger.warn("\tbackoff {} millis ...", backoffInterval);
            Thread.sleep(backoffInterval);
            logger.warn("\tretrying");
            return requestLock(container, db);
        } else {
            // normal case when only locked by us
            logger.warn("\tlock obtained, token: {}", lockItem.getToken());
            return lockItem;
        }
    }
    private static boolean checkIfLocked(CosmosAsyncContainer container, String db) {
        return checkIfLocked(container, db, null);
    }

    public static List<DbLockItem> queryLocks(CosmosAsyncContainer container, String db) {
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setPartitionKey(new PartitionKey(db));

        // Simple query with a single property equality comparison
        // in SQL with SQL parameterization instead of inlining the
        // parameter values in the query string

        ArrayList<SqlParameter> paramList = new ArrayList<>();
        paramList.add(new SqlParameter("@status", "ACTIVE"));
        SqlQuerySpec querySpec = new SqlQuerySpec(
                "SELECT * FROM DbLockC1 l WHERE (l.status = @status)",
                paramList);

        return executeQueryWithQuerySpec(container, querySpec);
    }

    private static boolean checkIfLocked(CosmosAsyncContainer container, String db, String lockToken) {
        List<DbLockItem> lockItems = queryLocks(container, db);

        if (StringUtils.isNoneBlank(lockToken)) {
            // check if there are lock items with a different token than `lockToken`
            List<DbLockItem> otherLockItems = lockItems.stream()
                    .filter(item -> !lockToken.equalsIgnoreCase(item.getToken()))
                    .collect(Collectors.toList());
            List<String> lockTokens = lockItems.stream().map(DbLockItem::getToken).collect(Collectors.toList());
            logger.info("Lock tokens: {}", String.join(",", lockTokens));
            boolean lockedByOthers = !otherLockItems.isEmpty();
            if (lockedByOthers) {
                logger.warn("!! also locked by others !!");
            }
            return lockedByOthers;
        } else {
            return !lockItems.isEmpty();
        }
    }

    private static List<DbLockItem> executeQueryWithQuerySpec(CosmosAsyncContainer container, SqlQuerySpec querySpec) {
        logger.info("Execute query {}",querySpec.getQueryText());

        CosmosPagedFlux<DbLockItem> lockItems = container.queryItems(querySpec, new CosmosQueryRequestOptions(), DbLockItem.class);

        Iterable<DbLockItem> itActiveLockItem = lockItems.map(resp -> resp).toIterable();
        List<DbLockItem> activeLocks = new ArrayList<DbLockItem>();
        itActiveLockItem.forEach(activeLocks::add);
        // Print
        if (!activeLocks.isEmpty()) {
            String json = GSON.toJson(activeLocks.get(0));
            logger.info("First query result: {}", json);
        } else {
            logger.warn("No item found!");
        }
        return activeLocks;
    }
}
