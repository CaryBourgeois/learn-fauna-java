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
package com.fauna;

/*
 * These imports are for basic functionality around logging and JSON handling and Futures.
 * They should best be thought of as a convenience items for our demo apps.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

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

public class Lesson3 {
    private static final Logger logger = LoggerFactory.getLogger(Lesson2.class);

    private static ObjectMapper mapper = new ObjectMapper();

    private static String toPrettyJson(Value value) throws JsonProcessingException {
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    }

    private static String toJson(Value value) throws JsonProcessingException {
        return mapper.writeValueAsString(value);
    }

    public static void main(String[] args)  throws Exception {
        /*
         * Create an admin connection to FaunaDB.
         *
         * This is only used if you are using your own Developers or Enterprise edition of FaunaDB
         */
        FaunaClient adminClient = FaunaClient.builder()
                .withEndpoint("http://127.0.0.1:8443")
                .withSecret("secret")
                .build();
        logger.info("Connected to FaunaDB as Admin!");

        /*
         * Create a database
         */
        String dbName = "LedgerExample";

        Value result;

        result = adminClient.query(
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
        String secret = result.at("secret").to(String.class).get();
        logger.info("DB {} secret: {}", dbName, secret);

        /*
         * Create the DB specific DB client using the DB specific key just created.
         */
        FaunaClient client = adminClient.newSessionClient(secret);
        logger.info("Connected to FaunaDB as server for DB {}!", dbName);

        /*
         * Create an class to hold customers
         */
        result = client.query(
                CreateClass(Obj("name", Value("customers")))
        ).get();
        logger.info("Created customers class :: {}", toPrettyJson(result));

        /*
         * Create two indexes here. The first index is to query customers when you know specific id's.
         * The second is used to query customers by range. Examples of each type of query are presented
         * below.
         */
        result = client.query(
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
                                        "name", Value("customer_id_filter"),
                                        "source", Class(Value("customers")),
                                        "unique", Value(true),
                                        "values", Arr(
                                                Obj("field", Arr(Value("data"), Value("id"))),
                                                Obj("field", Arr(Value("ref")))
                                        )
                                )
                            )
                        )
        ).get();
        logger.info("Created \'customer_by_id\' index & \'customer_id_filter\' index :: {}", toPrettyJson(result));


        /*
         * Create 20 customer records with ids from 1 to 20
         */
        List<Integer> range = IntStream
                .rangeClosed(1, 20)
                .boxed()
                .collect(Collectors.toList());
        result = client.query(
                Map(Value(range),
                        Lambda( Value("id"),
                                Create(
                                        Class(Value("customers")),
                                        Obj("data",
                                                Obj("id", Var("id"), "balance", Multiply(Var("id"), Value(10)))
                                        )
                                )
                        )
                )
        ).get();

        /*
         * Read a single record and return the data it holds
         * We saw this from the previous Lesson code
         */
        int custID = 1;
        result = client.query(
                Select(Value("data"), Get(Match(Index("customer_by_id"), Value(custID))))
        ).get();
        logger.info("Read \'customer\' {}: \n{}", custID, toPrettyJson(result));

        /*
         * Here is a more general use case where we retrieve multiple class references
         * by id and return the actual data underlying them.
         */
        result = client.query(
                Map(
                        Paginate(
                                Union(
                                        Match(Index("customer_by_id"), Value(1)),
                                        Match(Index("customer_by_id"), Value(3)),
                                        Match(Index("customer_by_id"), Value(8))
                                )
                        ),
                        Lambda(Value("x"), Select(Value("data"), Get(Var("x"))))
                )
        ).get();
        logger.info("Union specific \'customer\' 1, 3, 8: \n{}", toPrettyJson(result));

        /*
         * Finally a much more general use case where we can supply any number of id values
         * and return the data for each.
         */
        range.clear();
        range.add(1);
        range.add(3);
        range.add(6);
        range.add(7);
        result = client.query(
                Map(
                        Paginate(
                                Union(
                                        Map(
                                                Value(range),
                                                Lambda(Value("y"), Match(Index("customer_by_id"), Var("y")))
                                        )
                                )
                        ),
                        Lambda(Value("x"), Select(Value("data"), Get(Var("x"))))
                )
        ).get();
        logger.info("Union variable \'customer\' {}: \n{}", range, toPrettyJson(result));

        /*
         * In this example we use the values based filter 'customer_id_filter'.
         * using this filter we can query by range. This is an example of returning
         * all the values less than(<) or before 5. The keyword 'after' can replace
         * 'before' to yield the expected results.
         */
        result = client.query(
                Map(
                      Paginate(Match(Index("customer_id_filter"))).before(Value(5)),
                        Lambda(Value("x"), Select(Value("data"), Get(Select(Value(1), Var("x")))))
                )
        ).get();
        logger.info("Query for id\'s < 5 : {}", toPrettyJson(result));

        /*
         * Extending the previous example to show getting a range between two values.
         */
        result = client.query(
                Map(
                        Filter(Paginate(Match(Index("customer_id_filter"))).before(Value(11)),
                                Lambda(Value("y"), LTE(Value(5), Select(Value(0), Var("y"))))),
                        Lambda(Value("x"), Select(Value("data"), Get(Select(Value(1), Var("x")))))
                )

        ).get();
        logger.info("Query for id\'s > 5  and < 11 : {}", toPrettyJson(result));

        /*
         * Read all the records that we created.
         * Use a small'ish page size so that we can demonstrate a paging example.
         *
         * NOTE: after is inclusive of the value.
         */
        int cursorPos = 1;
        int pageSize = 8;
        boolean morePages = false;
        do {
            result = client.query(
                    Map(
                            Paginate(Match(Index("customer_id_filter")))
                                    .after(Value(cursorPos))
                                    .size(Value(pageSize)),
                            Lambda(Value("x"), Select(Value("data"), Get(Select(Value(1), Var("x")))))
                    )
            ).get();

            logger.info("Page through id\'s >= {}  and < {} : {}", cursorPos, cursorPos+pageSize, toJson(result));

            JsonNode node = mapper.readValue(toJson(result), JsonNode.class);
            if (node.findPath("after").toString().length() > 0) {
                cursorPos += pageSize;
                morePages = true;
            } else {
                morePages = false;
            }
        } while (morePages);

        /*
         * Just to keep things neat and tidy, close the client connections
         */
        client.close();
        logger.info("Disconnected from FaunaDB as server for DB {}!", dbName);
        adminClient.close();
        logger.info("Disconnected from FaunaDB as Admin!");

        // add this at the end of execution to make things shut down nicely
        System.exit(0);
    }
}
