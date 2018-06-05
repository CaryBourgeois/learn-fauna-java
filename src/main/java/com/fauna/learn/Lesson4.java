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

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.Random;

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


public class Lesson4 {
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
        logger.info("DB {} secret: {}", dbName, dbSecret);

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

    private static void createClasses(FaunaClient client) throws Exception {
        /*
         * Create an class to hold customers
         */
        Value result = client.query(
                Arr(
                        CreateClass(Obj("name", Value("customers"))),
                        CreateClass(Obj("name", Value("transactions")))
                )
        ).get();
        logger.info("Created class 'customer' & 'transactions' :: {}", toPrettyJson(result));
    }

    private static void createIndices(FaunaClient client) throws Exception {
        /*
         * Create two indexes here. The first index is to query customers when you know specific id's.
         * The second is used to query customers by range. Examples of each type of query are presented
         * below.
         */
        Value result = client.query(
                Arr(
                        CreateIndex(
                                Obj(
                                        "name", Value("customer_by_id"),
                                        "source", Class(Value("customers")),
                                        "unique", Value(true),
                                        "terms", Arr(Obj("field", Arr(Value("data"), Value("id"))))
                                )
                        ),
                        CreateIndex(
                                Obj(
                                        "name", Value("transactions_by_uuid"),
                                        "source", Class(Value("transactions")),
                                        "unique", Value(true),
                                        "values", Arr(
                                                Obj("field", Arr(Value("data"), Value("uuid"))),
                                                Obj("field", Arr(Value("ref")))
                                        )
                                )
                        )
                )
        ).get();
        logger.info("Created indices 'customer_by_id' & 'customer_id_filter' :: {}", toPrettyJson(result));
    }

    private static List<Value> createCustomers(FaunaClient client, int numCustomers, int initBalance) throws Exception {
        /*
         * Create 'numCustomers' customer records with ids from 1 to 'numCustomers'
         */
        List<Integer> range = IntStream
                .rangeClosed(1, numCustomers)
                .boxed()
                .collect(Collectors.toList());
        Value result = client.query(
                Map(Value(range),
                        Lambda( Value("id"),
                                Create(
                                        Class(Value("customers")),
                                        Obj("data",
                                                Obj("id", Var("id"), "balance", Value(initBalance))
                                        )
                                )
                        )
                )
        ).get();

        List<Value> custRefs = result.collect(Field.at("ref")).asList();

        logger.info("Created {} new customers with balance: {}", numCustomers, initBalance);

        return custRefs;
    }

    private static int sumCustBalances(FaunaClient client, List<Value> custRefs) throws Exception {
        /*
         * This is going to take the customer references that were created during the
         * createCustomers routine and aggregate all the balances for them. We could so this,
         * and probably would, with class index. In this case we want to take this approach to show
         * how to use references.
         */
        Value result = client.query(
                Map(Arr(custRefs),
                        Lambda(Value("x"), Select(Value("data"), Get(Var("x")))))
        ).get();

        int balance = 0;

        List<Integer> data = result.collect(Field.at("balance").to(Codec.INTEGER)).asList();
        if (!data.isEmpty()) {
            for (Integer d : data) {
                balance += d;
            }
        }

        logger.info("Customer Balance Sum: {}", balance);

        return balance;
    }

    public static void createTransaction(FaunaClient client, int numCustomers, int maxTxnAmount) throws Exception {
        /*
         * This method is going to create a random transaction that moves a random amount
         * from a source customer to a destination customer. Prior to committing the transaction
         * a check will be performed to insure that the source customer has a sufficient balance
         * to cover the amount and not go into an overdrawn state.
         */
        Random random = new Random();

        String uuid = UUID.randomUUID().toString();

        int sourceID = random.nextInt(numCustomers) + 1;
        int destID = random.nextInt(numCustomers) + 1;
        while (sourceID == destID) {
            destID = random.nextInt(numCustomers) + 1;
        }
        int amount = random.nextInt(maxTxnAmount) + 1;

        Value result = client.query(
                Let (
                        "sourceCust", Get(Match(Index("customer_by_id"), Value(sourceID))),
                        "destCust" , Get(Match(Index("customer_by_id"), Value(destID)))).in(
                        Let (
                                "sourceBalance", Select(Arr(Value("data"), Value("balance")), Var("sourceCust")),
                                "destBalance", Select(Arr(Value("data"), Value("balance")), Var("destCust"))).in(
                                Let (
                                        "newSourceBalance", Subtract(Var("sourceBalance"), Value(amount)),
                                        "newDestBalance", Add(Var("destBalance"), Value(amount))).in(
                                        If (
                                                GTE (Var("newSourceBalance"), Value(0)),
                                                Do (
                                                        Create (
                                                                Class(Value("transactions")), Obj("data",
                                                                        Obj("uuid", Value(uuid),
                                                                                "sourceCust", Select(Arr(Value("data"), Value("id")), Var("sourceCust")),
                                                                                "destCust", Select(Arr(Value("data"), Value("id")), Var("destCust")),
                                                                                "amount", Value(amount)
                                                                        )
                                                                )),
                                                        Update (Select(Value("ref"), Var("sourceCust")), Obj("data", Obj("balance", Var("newSourceBalance")))),
                                                        Update (Select(Value("ref"), Var("destCust")), Obj("data", Obj("balance", Var("newDestBalance"))))
                                                ),
                                                Value("Error. Insufficient funds.")
                                        )
                                )
                        )
                )
        ).get();
    }

    public static void main(String[] args)  throws Exception {
        String dcURL = "http://127.0.0.1:8443";
        String secret = "secret";
        String dbName = "LedgerExample";

        String dbSecret = createDatabase(dcURL, secret, dbName);

        FaunaClient client = createDBClient(dcURL, dbSecret);

        createClasses(client);

        createIndices(client);

        List<Value> custRefs = createCustomers(client, 50, 100);

        sumCustBalances(client, custRefs);

        for (int i = 0; i < 100; i++){
            createTransaction(client, 50, 10);
        }

        sumCustBalances(client, custRefs);

        /*
         * Just to keep things neat and tidy, close the client connections
         */
        client.close();
        logger.info("Disconnected from FaunaDB as server for DB {}!", dbName);

        // add this at the end of execution to make things shut down nicely
        System.exit(0);
    }
}
