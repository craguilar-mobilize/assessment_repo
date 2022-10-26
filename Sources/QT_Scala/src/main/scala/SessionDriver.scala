package com.mobilize.spark

import com.mobilize.spark.Credentials._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SessionDriver {

  val session = SparkSession.builder()
    .master("local")
    .appName("Test Snowpark_Extensions")
    .getOrCreate()

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

  def read(tablename:String) = {
    session.read.
      format("net.snowflake.spark.snowflake").
      option("sfUrl",s"https://$ACCOUNT.snowflakecomputing.com:443").
      option("sfAccount", ACCOUNT).
      option("sfUser", USER).
      option("sfPassword", PASSWORD).
      option("sfSchema",SCHEMA).
      option("sfDatabase",DB).
      option("sfWarehouse", WAREHOUSE).
      option("sfRole",ROLE).
      option("query", s"select * from $tablename").load()
    /* session.table(tablename) */
  }

  def write(df: DataFrame, tablename:String){
    df.write.
      format("net.snowflake.spark.snowflake").
      option("sfUrl",s"https://$ACCOUNT.snowflakecomputing.com:443").
      option("sfAccount", ACCOUNT).
      option("sfUser", USER).
      option("sfPassword", PASSWORD).
      option("sfSchema",SCHEMA).
      option("sfDatabase",DB).
      option("sfWarehouse", WAREHOUSE).
      option("sfRole",ROLE).
      option("truncate_table", "ON").
      option("usestagingtable" , "OFF").
      option("dbtable", tablename).mode("overwrite").save()
    /*df.write.mode("overwrite").saveAsTable(tablename)*/
  }

}
