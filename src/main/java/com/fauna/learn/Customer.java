package com.fauna.learn;

import com.faunadb.client.types.FaunaConstructor;
import com.faunadb.client.types.FaunaField;
import com.faunadb.client.types.Field;

public class Customer {

    static final Field<Customer> CUSTOMER_FIELD = Field.at("data").to(Customer.class);

    @FaunaField private int id;
    @FaunaField private int balance;

    @FaunaConstructor
    public Customer(@FaunaField("id") int id, @FaunaField("balance") int balance) {
        this.id = id;
        this.balance = balance;
    }

    public int getId() {
        return id;
    }

    public int getBalance() {
        return balance;
    }
}
