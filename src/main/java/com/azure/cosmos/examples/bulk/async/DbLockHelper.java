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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DbLockHelper {
    private static final Gson GSON = new Gson();
    private static int SLEEP_INTERVAL = 5000;
    private static Logger logger = LoggerFactory.getLogger(DbLockHelper.class);

    public static boolean releaseLock(CosmosAsyncContainer container, DbLockItem currLock) throws Exception {
        if (checkIfLocked(container, currLock.getToken())) {
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

    public static boolean dbgDeleteLockItem(CosmosAsyncContainer container, DbLockItem itemToDelete) {
        container.deleteItem(itemToDelete.getId(), new PartitionKey(itemToDelete.getDb())).block();
        return true;
    }

    public static DbLockItem requestLock(CosmosAsyncContainer container) throws Exception {
        DbLockItem lockItem = DbLockItem.requestLock("fakedb");
        logger.warn("Checking if locked");
        while (checkIfLocked(container)) {
            Thread.sleep(SLEEP_INTERVAL);
        }
        logger.warn("Trying lock db");
        lockItem.setLockDt(System.currentTimeMillis());
        container.createItem(lockItem).block();
        if (checkIfLocked(container, lockItem.getToken())) {
            // todo: rare case when it's also locked by others!!
            logger.error("TODO: rare case when it's also locked by others!!");
            return null;
        } else {
            // normal case when only locked by us
            logger.warn("\tlock obtained, token: {}", lockItem.getToken());
            return lockItem;
        }
    }
    private static boolean checkIfLocked(CosmosAsyncContainer container) {
        return checkIfLocked(container, null);
    }

    public static List<DbLockItem> queryLocks(CosmosAsyncContainer container) {
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setPartitionKey(new PartitionKey("db"));

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

    private static boolean checkIfLocked(CosmosAsyncContainer container, String lockToken) {
        List<DbLockItem> lockItems = queryLocks(container);

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

//        // Query using two properties within each document. WHERE Id = "" AND Address.City = ""
//        // notice here how we are doing an equality comparison on the string value of City
//
//        paramList = new ArrayList<SqlParameter>();
//        paramList.add(new SqlParameter("@id", "AndersenFamily"));
//        paramList.add(new SqlParameter("@city", "Seattle"));
//        querySpec = new SqlQuerySpec(
//                "SELECT * FROM Families f WHERE f.id = @id AND f.Address.City = @city",
//                paramList);
//
//        executeQueryWithQuerySpecPrintSingleResult(querySpec);
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
