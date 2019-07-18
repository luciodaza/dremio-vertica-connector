# Dremio Vertica ARP Connector

The Vertica connector allows Dremio to connect to and query data in a Vertica database. This can then allow you to build custom reports, dashboards, or even just ad-hoc SQL via your client tool of choice. Note that it does require a third-party JDBC driver that is not free, but does allow a free trial.


# ARP Overview

The Advanced Relational Pushdown (ARP) Framework allows for the creation of Dremio plugins for any data source which has a JDBC driver and accepts SQL 
as a query language. It allows for a mostly code-free creation of a plugin, allowing for modification of queries issued 
by Dremio using a configuration file.

There are two files that are necessary for creation of an ARP-based plugin: the storage plugin configuration, which 
is code, and the plugin ARP file, which is a YAML (https://yaml.org/) file.

The storage plugin configuration file tells Dremio what the name of the plugin should be, what connection options 
should be displayed in the source UI, what the name of the ARP file is, which JDBC driver to use and how to make a 
connection to the JDBC driver.

The ARP YAML file is what is used to modify the SQL queries that are sent to the JDBC driver, allowing you to specify 
support for different data types and functions, as well as rewrite them if tweaks need to be made for your specific 
data source. 

## ARP File Format

The ARP file is broken down into several sections:

**metadata**
- This section outlines some high level metadata about the plugin.

**syntax**
- This section allows for specifying some general syntax items like the identifier quote character.

**data_types**
- This section outlines which data types are supported by the plugin, their names as they appear in the JDBC driver, and how they map to Dremio types.

**relational_algebra** - This section is divided up into a number of other subsections:

- **aggregation**
  - Specify what aggregate functions, such as SUM, MAX, etc, are supported and what signatures they have. You can also specify a rewrite to alter the SQL for how this is issued.
- **except/project/join/sort/union/union_all/values**
  - These sections indicate if the specific operation is supported or not.
- **expressions**
  - This section outlines general operations that are supported. The main sections are:
- **operators**
  - Outlines which scalar functions, such as SIN, SUBSTR, LOWER, etc, are supported, along with the signatures of these functions which are supported. Finally, you can also specify a rewrite to alter the SQL for how this is issued.
- **variable_length_operators**
  - The same as operators, but allows specification of functions which may have a variable number of arguments, such as AND and OR.

If an operation or function is not specified in the ARP file, then Dremio will handle the operation itself. Any operations which are indicated as supported but need to be stacked on operations which are not will not be pushed down to the SQL query.


## Building and Installation

1. In root directory with the pom.xml file run `mvn clean install`
2. Take the resulting .jar file in the target folder and put it in the $DREMIO_HOME/jars folder in Dremio
3. Download the [Salesforce JDBC driver from CData](https://www.cdata.com/drivers/salesforce/jdbc/) and put in in the $DREMIO_HOME/jars/3rdparty folder. Note you will need to sign up for a trial usage of the driver or pay for it if needing to use long term.
4. Restart Dremio


## Steps to create your own custom ARP connector

Follow the below steps to modify this Salesforce connector to create your own custom ARP connector.

Note that the ARP framework requires a JDBC Driver for the source to allow Dremio to connect to it.

1. Refactor VerticaConf.java to be {CustomSource}Conf.java, modifying all the instances of "Vertica" to your source name (including the driver class name)
2. Consult the documentation of the JDBC driver for how to construct the JDBC connection string. use this string construction in toJdbcConnectionString(). use the variables needed to add metadata labels and properties to the class
3. Refactor vertica-arp.yaml to be {customsource}-arp.yaml, modifying all instances of "vertica" to your source name
4. Download the JDBC driver for the source
5. Use the JDBC driver to connect to a sample source via sql client (e.g. dbeaver, dbvizualizer)
6. View the source documentation or view the data types used in the tables and views used to get a list of the data types you need to map to dremio data types
7. Update the rest of the file to add/modify any functions, operator behavior, etc. specific to the source. You can use official Dremio connectors built on ARP, such as [Postgres](https://github.com/dremio/dremio/blob/master/community/plugins/jdbc/src/main/resources/arp/implementation/postgresql-arp.yaml) and [Oracle](https://github.com/dremio/dremio/blob/master/community/plugins/jdbc/src/main/resources/arp/implementation/oracle-arp.yaml) to view additional options available. 
8. Update README.md
9. To build, run `mvn clean install`. get the jar in the target dir, and copy that to $DREMIO_HOME/jars (on all nodes if running in cluster mode)
10. Copy the JDBC driver to $DREMIO_HOME/jars/3rdparty (on all nodes if running in cluster mode)
11. Restart dremio 