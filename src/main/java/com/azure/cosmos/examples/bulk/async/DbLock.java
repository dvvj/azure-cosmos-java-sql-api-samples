// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.bulk.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class DbLock {

    private static Logger logger = LoggerFactory.getLogger(DbLock.class);
    private final String databaseName = "DbLockT1";
    private final String containerName = "DbLockC1";
    private CosmosAsyncClient client;
    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    public static void main(String[] args) {
        DbLock p = new DbLock();


        try {
            logger.info("Starting ASYNC main");
            p.getStartedDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            // p.shutdown();
        }
    }

    public void close() {
        client.close();
    }

    private void getStartedDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ArrayList<String> preferredRegions = new ArrayList<>();
        preferredRegions.add("West US");

        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the
        //  application

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION).buildAsyncClient();

        //  </CreateAsyncClient>

        // prepareDbContainer();
        database = client.getDatabase(databaseName);
        container = database.getContainer(containerName);

        dbgDeleteLocks();

        DbLockItem lockItem = DbLockHelper.requestLock(container);
        Thread.sleep(5000); // doing stuff
        DbLockHelper.releaseLock(container, lockItem);
    }

    private void dbgDeleteLocks() {
        List<DbLockItem> locks = DbLockHelper.queryLocks(container);
        locks.forEach(l -> DbLockHelper.dbgDeleteLockItem(container, l));
    }

    private void prepareDbContainer() {

        createDatabaseIfNotExists();
        createContainerIfNotExists();

    }

    private void createDatabaseIfNotExists() {
        logger.info("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosDatabaseResponse> databaseIfNotExists = client.createDatabaseIfNotExists(databaseName);
        databaseIfNotExists.flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            logger.info("Checking database " + database.getId() + " completed!\n");
            return Mono.empty();
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private void createContainerIfNotExists() {
        logger.info("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/db");
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(25000);
        Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties,
                throughputProperties);

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        //Modify existing container
        containerProperties = cosmosContainerResponse.getProperties();
        Mono<CosmosContainerResponse> propertiesReplace = container.replace(containerProperties,
                new CosmosContainerRequestOptions());
        propertiesReplace.flatMap(containerResponse -> {
            logger.info("setupContainer(): Container " + container.getId() + " in " +
                    database.getId() + "has been " + "updated with it's new " + "properties.");
            return Mono.empty();
        }).onErrorResume((exception) -> {
            logger.error("setupContainer(): Unable to update properties for container " + container.getId()
                    + " in " + "database " + database.getId() + ". e: " + exception.getLocalizedMessage());
            return Mono.empty();
        }).block();

    }

    private void shutdown() {
        try {
            //Clean shutdown
            logger.info("Deleting Cosmos DB resources");
            logger.info("-Deleting container...");
            if (container != null) container.delete().subscribe();
            logger.info("-Deleting database...");
            if (database != null) database.delete().subscribe();
            logger.info("-Closing the client...");
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack " +
                    "trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done.");
    }


}
