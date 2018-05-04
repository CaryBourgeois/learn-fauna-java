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
package com.fauna.learnfauna;

/*
 * These imports are for basic functionality around logging and JSON handling and Futures.
 * They should best be thought of as a convenience items for our demo apps.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonProcessingException;

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

public class Lesson1 {
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
        logger.info("Succesfully connected to FaunaDB as Admin!");

        /*
         * Create a database
         */
        String dbName = "TestDB";

        Value result;

        result = adminClient.query(
                CreateDatabase(Obj("name", Value(dbName)))
        ).get();
        logger.info("Successfully created database: {} :: \n{}", dbName, toPrettyJson(result));

        /*
         * Delete the Database that we created
         */
        result = adminClient.query(
                If(
                        Exists(Database(dbName)),
                        Delete(Database(dbName)),
                        Value(true)
                )
        ).get();
        logger.info("Successfully deleted database: {} :: \n{}", dbName, toPrettyJson(result));

        /*
         * Just to keep things neat and tidy, close the client connection
         */
        adminClient.close();
        logger.info("Succesfully disconnected from FaunaDB as Admin!");

        // add this at the end of execution to make things shut down nicely
        System.exit(0);
    }
}
