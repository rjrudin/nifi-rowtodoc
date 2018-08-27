Rows to documents with Apache NiFi
=========

This project provides some NiFi processors to read rows from an RDBMS, join them together, and then serialize them 
into JSON (or, soon, XML). The intent is to write these [MarkLogic](http://www.marklogic.com) using the 
[PutMarkLogic](https://developer.marklogic.com/code/apache-nifi) processor, but you could do anything with the documents.

Trying out the processors
=========

You can find a nar file containing the processors on [the releases page](/releases). Copy that to the processor 
directory of your NiFi installation (e.g. on a Mac, /usr/local/Cellar/nifi/(version)/libexec/lib). Start (or restart)
NiFi, and you'll now be able to use the following processors:

- ExecuteSQLToColumnMaps
- ExecuteChildQueriesOnColumnMaps
- ConvertColumnMapsToJSON (an XML one will soon exist)

Each processor is described below, with the [MySQL Sakila dataset](https://dev.mysql.com/doc/sakila/en/sakila-structure.html) 
used as an example. The goal is to combine all of the data from the Customer, Rental, and Payment tables into Customer
JSON documents, where each Customer has a "rentals" array of Rental objects, and each Rental has a "payments" array of 
Payment objects.  

**ExecuteSQLToColumnMaps**

Add this processor to run an initial query of "select * from Customer" (configure this in the Properties tab of the 
processor). You'll need to configure a standard NiFi 
[DBCPConnectionPool](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-dbcp-service-nar/1.5.0/org.apache.nifi.dbcp.DBCPConnectionPool/index.html), 
which is a controller service. 

This processor also has a "Batch size" property that defaults to a value of 100. This controls how many rows are read
by this processor from the JDBC ResultSet that is created from the configured query. Once this many rows are read, they're
packaged into a new FlowFile that is sent to the next processor. Each row is first converted into a "column map" - a 
Map<String, Object> using the Spring JDBC [ColumnMapRowMapper](https://docs.spring.io/spring-framework/docs/5.0.8.RELEASE/javadoc-api/org/springframework/jdbc/core/ColumnMapRowMapper.html). 
The maps are then added a list, so it's a List<Map<String,Object>> that's written as a byte array to the next processor.

**ExecuteChildQueriesOnColumnMaps**

Add this processor to run "child queries" to populate the rental arrays on each of the incoming column maps (each of which 
represents a customer), and to populate the payments arrays on each rental. Connect the ExecuteSQLToColumnMaps processor
to this one. 

This processor first needs the same DBCPConnectionPool configured on it as the first processor (ExecuteSQLToColumnMaps). 

You then need to configure the "Child query JSON" property. This is a JSON object that defines the child queries to execute. 
Here's the JSON that is entered for the child queries on rentals and payments:

    {
      "primaryKeyColumnName": "customer_id",
      "childQueries": [
        {
          "query": "select * from Rental",
          "primaryKeyColumnName": "rental_id",
          "propertyName": "rentals",
          "foreignKeyColumnName": "customer_id",
          "childQueries": [
            {
              "query": "select * from Payment",
              "propertyName": "payments",
              "foreignKeyColumnName": "rental_id"
            }
          ]
        }
      ]
    } 

Explanation of the properties in this JSON:

1. primaryKeyColumnName = this defines the name of the primary key column for the "parent" objects - i.e. the primary 
key from the Customer table.
1. childQueries = an array of one to many JSON objects, each defined below
1. query = the query for selected rows from the child table. This can include a "where" clause. The processor will then 
expand this query by including an "in" clause that constrains the foreign key column against the primary key value of 
each of the incoming records - so in this example, a "customer_id in (value1, value2, etc)" clause. 
1. primaryKeyColumnName = this is only needed if this child query object has a "childQueries" array
1. foreignKeyColumnName = this is used to construct the "in" clause 
1. childQueries = optional array of child query JSON objects. In this example, the "select * from Payment" query will be
expanded to include a "where rental_id in (value1, value2, etc)" clause where the sequence of values is the set of 
rental IDs that were read in by the "select * from Rental" query.

You can nest child queries to an infinite level.

This processor will then pass the List<Map<String, Object>> on to the next processor.

**ConvertColumnMapsToJSON**

Add this processor to convert each column map in the incoming List<Map<String, Object>> to a JSON document using the 
Java [Jackson](https://github.com/FasterXML/jackson) library. This processor doesn't yet have any properties; in the 
future, it could have properties that allow you to configure the Jackson ObjectMapper that is used under the hood to 
convert each Map<String, Object> into a string of JSON.

Connect the ExecuteChildQueriesOnColumnMaps processor to this processor. 

**And finally**

To write documents to MarkLogic, use the PutMarkLogic processor from the [MarkLogic NiFi nar](https://github.com/marklogic/nifi-nars). 

Connect the "CONTENT" relationship of the ConvertColumnMapsToJSON processor to PutMarkLogic. Configure the "SUCCESS" 
relationship of ConvertColumnMapsToJSON to automatically terminate.

Another example - mapping a join table
=========

The Sakila schema has a many:many relationship between films and actors. This relationship is captured via the "film_actor"
join table. It's not likely that films and actors would be mapped to the same document - these both seem like first-class
nouns in a model that should be in separate documents. But for sake of example, let's look at how film documents can be 
created with actor data in them.

First, configure the ExecuteSQLToColumnMaps processor with the following simple query:

    select * from film

Next, to add actors to each film, configure the ExecuteChildQueriesOnColumnMaps processor with the following child query JSON:

    {
      "primaryKeyColumnName": "film_id",
      "childQueries": [
        {
          "query": "select a.* from actor a inner join film_actor fa on fa.actor_id = a.actor_id",
          "propertyName": "actors",
          "foreignKeyColumnName": "film_id"
        }
      ]
    }

That has a single child query that grabs all of the data from each actor row that matches any of the film column maps
that this processor receives in a single batch. Each actor row becomes a new column map that is stored under the "actors"
key of each film column map.

The ConvertColumnMapsToJSON processor can be left alone, as it doesn't yet have any configuration.

Finally, configure the PutMarkLogic processor to your liking - e.g. customize the collections, permissions, URI, etc. 

Running the flow will then produce "film" documents, each with an "actors" array that contains a JSON object per 
related actor.
