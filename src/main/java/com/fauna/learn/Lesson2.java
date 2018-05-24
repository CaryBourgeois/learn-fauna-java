/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fauna.learn;

/*
 * These imports are for basic functionality around logging and JSON handling and Futures.
 * They should best be thought of as a convenience items for our demo apps.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.google.common.base.Optional;

/*
 * These are the required imports for Fauna.
 *
 * For these examples we are using the 2.1.0 version of the JVM driver. Also notice that we aliasing
 * the query and values part of the API to make it more obvious we we are using Fauna functionality.
 *
 */
import com.faunadb.client.*;
import com.faunadb.client.types.*;
import static com.faunadb.client.query.Language.*;

public class Lesson2 {
    private static final Logger logger = LoggerFactory.getLogger(Lesson1.class);

    private static ObjectMapper mapper = getMapper();

    private static ObjectMapper getMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return mapper;
    }

    private static String toPrettyJson(Value value) throws JsonProcessingException {
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    }

    private static String createDatabase(String sURL, String secret , String dbName) throws Exception {
        /*
         * Create an admin connection to FaunaDB.
         *
         * This is only used if you are using your own Developers or Enterprise edition of FaunaDB
         */
        FaunaClient adminClient = FaunaClient.builder()
                .withEndpoint(sURL)
                .withSecret(secret)
                .build();
        logger.info("Connected to FaunaDB as Admin!");

        /*
         * Create a database
         */
        Value result = adminClient.query(
                Arr(
                        If(
                                Exists(Database(dbName)),
                                Delete(Database(dbName)),
                                Value(true)
                        ),
                        CreateDatabase(Obj("name", Value(dbName)))
                )
        ).get();
        logger.info("Created database: {} :: \n{}", dbName, toPrettyJson(result));

        /*
         * Create a key specific to the database we just created. We will use this to
         * create a new client we will use in the remainder of the examples.
         */
        result = adminClient.query(
                CreateKey(Obj("database", Database(Value(dbName)), "role", Value("server")))
        ).get();
        String dbSecret = result.at("secret").to(String.class).get();
        logger.info("DB {} secret: {}", dbName, secret);

        adminClient.close();
        logger.info("Disconnected from FaunaDB as Admin!");

        return dbSecret;
    }

    private static FaunaClient createDBClient(String dcURL, String secret) throws Exception {
        /*
         * Create the DB specific DB client using the DB specific key just created.
         */
        FaunaClient client = FaunaClient.builder()
                .withEndpoint(dcURL)
                .withSecret(secret)
                .build();
        logger.info("Connected to FaunaDB as server");

        return client;
    }

    private static void createSchema(FaunaClient client) throws Exception {
        /*
         * Create an class to hold customers
         */
        Value result = client.query(
                CreateClass(Obj("name", Value("customers")))
        ).get();
        logger.info("Created customers class :: {}", toPrettyJson(result));


        /*
         * Create an index to access customer records by id
         */
        result = client.query(
                CreateIndex(
                        Obj(
                                "name", Value("customer_by_id"),
                                "source", Class(Value("customers")),
                                "unique", Value(true),
                                "terms", Arr(Obj("field", Arr(Value("data"), Value("id"))))
                        )
                )
        ).get();
        logger.info("Created customer_by_id index :: {}", toPrettyJson(result));
    }

    private static void createCustomer(FaunaClient client, int custID, int balance) throws Exception {
        /*
         * Create a customer (record)
         */
        Value result = client.query(
                Create(
                        Class(Value("customers")),
                        Obj("data",
                                Obj("id", Value(custID), "balance", Value(balance))
                        )
                )
        ).get();
        logger.info("Create \'customer\' {}: \n{}", custID, toPrettyJson(result));
    }

    private static void readCustomer(FaunaClient client, int custID) throws Exception {
        /*
         * Read the customer we just created
         */
        Value result = client.query(
                Select(Value("data"), Get(Match(Index("customer_by_id"), Value(custID))))
        ).get();
        logger.info("Read \'customer\' {}: \n{}", custID, toPrettyJson(result));
    }

    private static void updateCustomer(FaunaClient client, int custID, int newBalance) throws Exception {
        /*
         * Update the customer
         */
        Value result = client.query(
                Update(
                        Select(Value("ref"), Get(Match(Index("customer_by_id"), Value(custID)))),
                        Obj("data",
                                Obj("balance", Value(newBalance))
                        )
                )
        ).get();
        logger.info("Update \'customer\' {}: \n{}", custID, toPrettyJson(result));
    }

    private static void deleteCustomer(FaunaClient client, int custID) throws Exception {
        /*
         * Delete the customer
         */
        Value result = client.query(
                Delete(
                        Select(Value("ref"), Get(Match(Index("customer_by_id"), Value(custID))))
                )
        ).get();
        logger.info("Delete \'customer\' {}: \n{}", custID, toPrettyJson(result));
    }

    public static void main(String[] args) throws Exception {

        String dcURL = "http://127.0.0.1:8443";
        String secret = "secret";
        String dbName = "LedgerExample";

        String dbSecret = createDatabase(dcURL, secret, dbName);

        FaunaClient client = createDBClient(dcURL, dbSecret);

        createSchema(client);

        createCustomer(client, 0, 100);

        readCustomer(client, 0);

        updateCustomer(client, 0, 200);

        readCustomer(client, 0);

        deleteCustomer(client, 0);

        /*
         * Just to keep things neat and tidy, close the client connections
         */
        client.close();
        logger.info("Disconnected from FaunaDB as server for DB {}!", dbName);

        // add this at the end of execution to make things shut down nicely
        System.exit(0);
    }
}

