package com.mobilize.spark
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
 object Credentials {
   val WAREHOUSE = "snowpark_test_wh"

   val USER = "spark1"

   val PASSWORD = "Test123."

   val DB = "snowpark_testdb"

   val ROLE = "snowpark_testdb_role"

   val ACCOUNT = "mobilize"

   val SCHEMA = "COMSCORE"
}