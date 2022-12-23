package com.mobilize.spark

import com.mobilize.spark.Credentials._
import com.snowflake.snowpark.{DataFrame, Session}
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object SessionDriver {
   val session = Session.builder.configFile("connection.properties")
   /*/*EWI: SPRKSCL1112 => org.apache.spark.sql.Builder.master is not supported*/
   .master("local")
   /*EWI: SPRKSCL1112 => org.apache.spark.sql.Builder.appName is not supported*/
   .appName("Test Snowpark_Extensions")*/
   .create

   /*
   lazy val session = Session.builder.configs(Map (
   "URL" -> s"https://$ACCOUNT.snowflakecomputing.com:443",
   "USER" -> USER,
   "PASSWORD" -> PASSWORD,
   "ROLE" -> ROLE,
   "WAREHOUSE" -> WAREHOUSE,
   "DB" -> DB,
   "SCHEMA" -> SCHEMA
   )).create
   */
   def read(tablename: String) = {
   session.sql(s"select * from $tablename")
      /*EWI: SPRKSCL1112 => Reader format value is not supported.format("net.snowflake.spark.snowflake")
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfUrl", s"https://$ACCOUNT.snowflakecomputing.com:443")
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfAccount", ACCOUNT)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfUser", USER)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfPassword", PASSWORD)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfSchema", SCHEMA)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfDatabase", DB)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfWarehouse", WAREHOUSE)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("sfRole", ROLE)
      EWI: SPRKSCL1112 => Reader option is not supported.
      option("query", s"select * from $tablename")
      /*EWI: SPRKSCL1112 => org.apache.spark.sql.DataFrameReader.load is not supported*/
      .load()*/
   /* session.table(tablename) */
   }

   def write(df: DataFrame, tablename: String) {
      df.write.mode("overwrite").saveAsTable(tablename)
      /*EWI: SPRKSCL1112 => Writer format value is not supported.format("net.snowflake.spark.snowflake")
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfUrl", s"https://$ACCOUNT.snowflakecomputing.com:443")
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfAccount", ACCOUNT)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfUser", USER)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfPassword", PASSWORD)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfSchema", SCHEMA)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfDatabase", DB)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfWarehouse", WAREHOUSE)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("sfRole", ROLE)
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("truncate_table", "ON")
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("usestagingtable", "OFF")
      EWI: SPRKSCL1112 => Writer option is not supported.
      option("dbtable", tablename)
      /*EWI: SPRKSCL1112 => org.apache.spark.sql.DataFrameWriter.save is not supported*/
      .save()*/
   /*df.write.mode("overwrite").saveAsTable(tablename)*/
   }
}