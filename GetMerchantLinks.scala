package com.intuit.sbg.ml.feature.creator.domains.payments.generators

// package imports
import com.intuit.sbg.ml.feature.creator.domains.payments.constants.{AuditToIhubMap, IDMaps, Tables}
import com.intuit.sbg.ml.feature.creator.domains.payments.transformers.CleanseStrings
import com.intuit.sbg.ml.feature.creator.domains.payments.utils.AggExprs
import com.intuit.sbg.ml.feature.creator.transformers.TransformerTrait
import com.intuit.sbg.ml.feature.creator.sparkutils.SparkSessionTrait
// spark imports
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._


// scala and java imports
import scala.collection.mutable
import scala.util.Random.nextInt
import java.sql.Date
import java.sql.Timestamp

object GetMerchantLinks extends SparkSessionTrait {
  /**
    *
    * @param df             dataframe
    * @param merchantKey    identifier the merchant id for df ex) ihub_merchant_id, account_number
    * @param primaryKey     unique identifier for df
    * @param dateTimeColumn datetime column to filter and compare between dataframes
    * @param startDate
    * @param endDate
    * @return               new dataframe
    *
    **/
  def run(df: DataFrame
          , merchantKey: String
          , dateTimeColumn: String
          //, startDate: String
          //, endDate: String
         ): DataFrame = {

    import spark.sqlContext.implicits._

    //TERMINOLOGY:

    //Velocity - Merchants have attributes which first appeared close in time (15d, 60d)
    //Weak  - Link combination indicates weak relationship (IP, address)
    //Fraud - Fraud related closure
    //Fraud Manu Decline - Fraud realted manual decline
    //Decline - Decline of any sort
    //Bad - Risky closure
    //_only - Only this type of link
    //Credit - Credit closure or decline
    //Individual/Household/Business - Link combination indicates that the merchants are same individual, household, business
    //Prin_lastname - Secondary link (will never appear by itself)


    // define negative reputation status list
    val negRepuList = List("closed_ato", "closed_compliance", "closed_financial"
      , "closed_first_party", "closed_frapp", "closed_other_risk"
      , "closed_third_party", "declined_auto_compliance", "declined_auto_credit", "declined_auto_fraud"
      , "declined_auto_fraud_kba"  ,"declined_auto_rup",  "declined_manual_compliance"
      , "declined_manual_credit", "declined_manual_fraud", "declined_manual_rup", "fraud_attempted"
      , "fraud_attempted_no_brf", "fraud_potential", "fraud_potential_no_brf")


    // define fraud reputation status list
    val fraudRepuList = List("closed_ato", "closed_first_party", "closed_frapp", "declined_auto_fraud"
      , "declined_auto_fraud_kba", "closed_third_party", "fraud_attempted", "declined_manual_fraud"
      , "fraud_attempted_no_brf", "fraud_potential", "fraud_potential_no_brf")

    // define declined status list
    val declineRepuList = List("declined_auto_compliance", "declined_auto_credit", "declined_auto_fraud"
      , "declined_auto_fraud_kba"  ,"declined_auto_rup",  "declined_manual_compliance", "declined_manual_credit"
      , "declined_manual_fraud", "declined_manual_rup")

    // credit bad list for closures and declines
    val creditBadList = List("closed_financial","closed_other_risk","declined_auto_credit","declined_manual_credit")

    //val lookBack = -1095 // how far back should links be considered
    val velocity_th1 = 15
    val velocity_th2 = 60

    val startEndDates = df.select(min(dateTimeColumn).as("startDate"),max(dateTimeColumn).as("endDate")).first()
    val startDate = startEndDates.get(0)
    val endDate = startEndDates.get(1)


    //get list of attributes to ignore
    val filter_list = Tables.filter_listRead.withColumn("attribute_filt",
      when(col("attribute_type_code") === "bankaccount" || col("attribute_type_code") === "govid",
        concat(col("attribute_type_code"), lit("_"), col("attribute_subtype_code"))).
        otherwise(col("attribute_type_code")))

    // read in reputation list //
    val reputation_list = Tables.reputation_list_pre.
      filter( col("source_type") === "pmnt").
      withColumnRenamed("start_date", "rep_start_date").
      withColumnRenamed("end_date", "rep_end_date").
      withColumnRenamed("source_id", "rep_risk_account_source_id").
      withColumn("rep_risk_account_source_type",
        when(col("source_type") === "pmnt", "pmnt").
          otherwise("payroll")).
      drop("source_type")


    //read in raw edge list
    val rawEdgeListRead = Tables.rawEdgeListRead_pre
      .where("subsource_name in ('experianalertquarterly','ihub_merchant','ihub_merchant_address','ihub_merchant_audit_det','ihub_merchant_bank','ihub_merchant_comm','ihub_merchant_contact','zoot.application_iovation_data','zoot.application_summary')  and risk_account_source_type = 'pmnt'")
      .filter(
        ($"attribute_type_code"==="email" && $"attribute_subtype_code"==="business") ||
          //($"attribute_type_code"==="email" && $"attribute_subtype_code"==="home") ||
          ($"attribute_type_code"==="prin" && $"attribute_subtype_code"==="prin") ||
          ($"attribute_type_code"==="bname" && $"attribute_subtype_code"==="legal") ||
          ($"attribute_type_code"==="bname" && $"attribute_subtype_code"==="dba") ||
          ($"attribute_type_code"==="prin_lastname" && $"attribute_subtype_code"==="prin") ||
          ($"attribute_type_code"==="phone" && $"attribute_subtype_code"==="business") ||
          ($"attribute_type_code"==="phone" && $"attribute_subtype_code"==="home") ||
          ($"attribute_type_code"==="device" && $"attribute_subtype_code"==="device") ||
          ($"attribute_type_code"==="govid"  && $"attribute_subtype_code"==="fein") ||
          ($"attribute_type_code"==="govid"  && $"attribute_subtype_code"==="ssn") ||
          ($"attribute_type_code"==="bankaccount" && $"attribute_subtype_code"==="dda") ||
          ($"attribute_type_code"==="ip" && $"attribute_subtype_code"==="ip") ||
          ($"attribute_type_code"==="address" && $"attribute_subtype_code"==="home") ||
          ($"attribute_type_code"==="address" && $"attribute_subtype_code"==="business") ||
          ($"attribute_type_code"==="address" && $"attribute_subtype_code"==="mail") ||
          ($"attribute_type_code"==="url" && $"attribute_subtype_code"==="url")
      )
      .filter(col("start_date")<=  to_timestamp(lit(endDate)))
      .drop("risk_account_source_type")
      .withColumn("attribute", when(length(col("attribute_subtype_code")) > 0 && col("attribute_type_code") =!= col("attribute_subtype_code") && col("attribute_subtype_code") =!= "prin",
        concat(col("attribute_type_code"), lit("_"), col("attribute_subtype_code"))).otherwise(col("attribute_type_code"))).
      withColumn("attribute_filt", when(col("attribute_type_code") === "bankaccount" || col("attribute_type_code") === "govid",
        concat(col("attribute_type_code"), lit("_"), col("attribute_subtype_code"))).otherwise(col("attribute_type_code")))
      //remove bad attributes
      .join(filter_list, Seq("attribute_filt", "attribute_id_value"), "leftanti").drop("attribute_filt")
      //remove duplicates due to attribute subsourcing
      .groupBy("attribute_id_value","risk_account_source_id","source_row_identifier","attribute_type_code","attribute_subtype_code","attribute")
      .agg(min("start_date").alias("start_date"),max("end_date").alias("end_date"),max("last_modified_date").alias("last_modified_date"))//to remove subsource duplicates

    //only care about merchants having at least one attribute shared with another merchant to reduce size
    val rawEdgeList_sub = rawEdgeListRead
      .groupBy(col("attribute"), col("attribute_id_value"))
      .agg(countDistinct("risk_account_source_id").as("MID_ct")).filter(col("MID_ct") > 1 && col("MID_ct") < 1000).drop("MID_ct")

    //remove any links that have too few or too many merchants associated with them based on the query above
    val rawEdgeList = rawEdgeListRead
      .join(rawEdgeList_sub, Seq("attribute", "attribute_id_value"), "inner")


    // filter raw edge list only with merchants in merchToProcess dataframe and append histdates and assign to table_left
    val table_left = rawEdgeList.join(df, df(merchantKey) === rawEdgeList("risk_account_source_id"), "inner" )
      .drop(merchantKey)
      .filter(col("start_date") < col(dateTimeColumn))
      .select(col("start_date"), col("end_date"), col("source_row_identifier")
        , col("risk_account_source_id"), col("attribute_type_code"), col("attribute_subtype_code")
        , col("attribute"), col("attribute_id_value"), col(dateTimeColumn))
      .withColumn("date_col", col(dateTimeColumn))
      .withColumn("year", year(col("date_col")))
      .withColumn("month", month(col("date_col"))).withColumn("day", dayofmonth(col("date_col"))).drop("date_col")
      .repartition(col("attribute"))

    //get right table for self join of the base table but filter
    val table_right = rawEdgeList.
      withColumn("year", year(col("start_date"))).
      withColumn("month", month(col("start_date"))).
      withColumn("day", dayofmonth(col("start_date"))).
      withColumnRenamed("start_date", "link_start_date").
      withColumnRenamed("end_date", "link_end_date").
      withColumnRenamed("risk_account_source_id", "link_risk_account_source_id").
      repartition($"attribute")


    // self join rawEdgList by defining table_left and table_right then enrich with attribute level velocity and lapse onboarding variables
    val selfJoin = table_left.join(table_right, table_left("attribute") === table_right("attribute")  && table_right("link_start_date") < table_left(dateTimeColumn) &&
      table_left("attribute_id_value") === table_right("attribute_id_value")  &&
      table_left("risk_account_source_id") =!= table_right("link_risk_account_source_id"), "inner").
      select(
        table_left("risk_account_source_id").as("seed_id")
        , table_left("attribute_type_code").as("attr_type")
        , table_left("attribute_subtype_code").as("attr_subtype")
        , table_left("attribute").as("attribute")
        , table_left("attribute_id_value").as("attr_value")
        , table_left("start_date").as("seed_start_date")
        , table_left("end_date").as("seed_end_date")
        , table_left(dateTimeColumn)
        , table_right("link_risk_account_source_id").as("link_id")
        , table_right("link_start_date")
        , table_right("link_end_date")
      )
      //velocity
      .withColumn("velocity_th1_flag", when( (abs(datediff(col("seed_start_date"), col("link_start_date") )) <= velocity_th1), 1).otherwise(0))
      .withColumn("velocity_th2_flag", when( (abs(datediff(col("seed_start_date"), col("link_start_date") )) <= velocity_th2), 1).otherwise(0))

    //roll up links based on m1 m2 and attribute type/value to just m1 m2
    val m1m2Init = selfJoin.groupBy(col("seed_id"), col(dateTimeColumn),
      col("link_id")).
      pivot("attribute", Seq("address_business", "address_home", "address_mail",
        "bankaccount_dda", "bname_dba", "bname_legal", "device", "email_business", "govid_fein", //"email_home",
        "govid_ssn", "ip", "phone_business", "phone_home",
        "prin", "prin_lastname", "url")).
      agg(count("attr_value").as("attr_count")).na.fill(0)
      //remove last name only links
      .filter(col("address_business") + col("address_home") + col("address_mail")
      + col("bankaccount_dda") + col("bname_dba") + col("bname_legal") + col("device") + col("email_business")  + col("govid_fein") + col("govid_ssn") //+ col("email_home")
      + col("ip") +  col("phone_business") + col("phone_home") + col("prin")
      + col("url") > 0)

    //check if the joins meet velocity thresholds
    val m1m2AuxVelocity = selfJoin.groupBy("seed_id",dateTimeColumn,"link_id").agg(sum("velocity_th1_flag").as("velocity_th1_ct"),sum("velocity_th2_flag").as("velocity_th2_ct"))
      .withColumn("velocity_th1_flag",when($"velocity_th1_ct">0,1).otherwise(0))
      .withColumn("velocity_th2_flag",when($"velocity_th2_ct">0,1).otherwise(0))
      .select("seed_id",dateTimeColumn,"link_id","velocity_th1_flag","velocity_th2_flag")

    //join on velocity table above to get velocity measures
    val m1m2WVel = m1m2Init.join(m1m2AuxVelocity,Seq("seed_id",dateTimeColumn,"link_id"),"inner")

    // enrich m1m2 with reputation column
    val m1m2Final = m1m2WVel.join(reputation_list,
      col("link_id") === col("rep_risk_account_source_id") && when(col("rep_end_date").isNotNull,
        col(dateTimeColumn) > col("rep_start_date") && col(dateTimeColumn) <= col("rep_end_date")).
        otherwise(col(dateTimeColumn) > col("rep_start_date" )), "left").
      drop( "rep_risk_account_source_id")
      //weak indicator
      .withColumn("link_flag",lit(1))
      .withColumn("ipOnly_flag",
        when(
          (col("address_business") + col("address_home") + col("address_mail")
            + col("bankaccount_dda") + col("bname_dba") + col("bname_legal") + col("device") + col("email_business") + col("govid_fein") + col("govid_ssn") //+ col("email_home")
            +  col("phone_business") + col("phone_home") + col("prin")
            + col("url") + col("prin_lastname")=== 0 &&  col("ip") > 0)
          , 1).otherwise(0))
      .withColumn("addressOnly_flag",
        when(
          (col("bankaccount_dda") + col("bname_dba") + col("bname_legal") + col("device") + col("email_business")  + col("govid_fein") + col("govid_ssn") //+ col("email_home")
            +  col("phone_business") + col("phone_home") + col("prin") + col("prin_lastname")+ col("ip")
            + col("url") === 0 && col("address_business") + col("address_home") + col("address_mail")  > 0)
          , 1).otherwise(0))
      //enrich bad flag
      .withColumn("bad_flag", when( col("reputation").isin(negRepuList:_*) , 1).otherwise(0)).
      withColumn("fraud_flag", when( col("reputation").isin(fraudRepuList:_*) , 1).otherwise(0)).
      //withColumn("fraud_weak_flag", when( col("reputation").isin(fraudRepuList:_*) && col("lastNameOnly_flag") === 1, 1).otherwise(0)).
      withColumn("fraud_weak_flag", when( col("reputation").isin(fraudRepuList:_*) && col("ipOnly_flag")+col("addressOnly_flag") > 0, 1).otherwise(0)).
      withColumn("fraud_FRAPP_flag", when( col("reputation") === "closed_frapp" , 1).otherwise(0)).
      withColumn("fraud_1P_flag", when( col("reputation") === "closed_first_party" , 1).otherwise(0)).
      withColumn("fraud_3P_flag", when( col("reputation") === "closed_third_party" , 1).otherwise(0)).
      //withColumn("fraud_manu_decline_weak_flag", when( col("reputation") === "declined_manual_fraud" && col("lastNameOnly_flag") === 1, 1).otherwise(0)).
      withColumn("fraud_manu_decline_weak_flag", when( col("reputation") === "declined_manual_fraud" && col("ipOnly_flag")+col("addressOnly_flag") > 0 === 1, 1).otherwise(0)).
      withColumn("fraud_manu_decline_flag", when( col("reputation") === "declined_manual_fraud" , 1).otherwise(0)).
      //withColumn("decline_weak_flag", when( col("reputation").isin(declineRepuList:_*) && col("lastNameOnly_flag") === 1, 1).otherwise(0)).
      withColumn("decline_weak_flag", when( col("reputation").isin(declineRepuList:_*) && col("ipOnly_flag")+col("addressOnly_flag") > 0 === 1, 1).otherwise(0)).
      withColumn("decline_flag", when( col("reputation").isin(declineRepuList:_*) , 1).otherwise(0)).
      //withColumn("credit_weak_flag", when( col("reputation").isin(creditBadList:_*) && col("lastNameOnly_flag") === 1, 1).otherwise(0)).
      withColumn("credit_weak_flag", when( col("reputation").isin(creditBadList:_*) && col("ipOnly_flag")+col("addressOnly_flag") > 0 === 1, 1).otherwise(0)).
      withColumn("credit_flag", when( col("reputation").isin(creditBadList:_*) , 1).otherwise(0)).
      withColumn("credit2_flag", when( col("reputation").isin(creditBadList:_*) && col("bankaccount_dda") + col("bname_dba") + col("bname_legal")+col("email_business") + col("govid_fein") + col("govid_ssn")+ col("prin")+ col("url") > 0, 1).otherwise(0)).

      drop("rep_start_date", "rep_end_date")
      .withColumn( "suspicious_links_1_flag",//dd+other
        (
          when(col("govid_ssn") + col("govid_fein" ) + col("bname_dba") + col("bname_legal") + col("prin")  === 0
            &&
            col("phone_business") + col("phone_home") + col("ip") + col("device") + col("bankaccount_dda") > 0 , 1).
            otherwise(0)))
      .withColumn( "suspicious_links_2_flag",//
        (
          when(col("govid_ssn") + col("govid_fein" ) + col("bname_dba") + col("bname_legal") + col("prin")  === 0
            && col("bankaccount_dda") > 0 && col("bad_flag") === 1 , 1).
            otherwise(0)))
      .withColumn( "suspicious_links_3_flag",
        (
          when(col("govid_ssn") + col("govid_fein" ) + col("bname_dba") +col("bname_legal") + col("prin")  === 0 &&
            col("bankaccount_dda") > 0
            , 1).
            otherwise(0)))
      .withColumn( "suspicious_links_4_flag",
        (
          when(col("govid_ssn") + col("govid_fein" ) + col("bname_dba") +col("bname_legal") + col("prin")  === 0 &&
            col("phone_business") + col("phone_home") + col("ip") + col("device") + col("bankaccount_dda") > 0 &&  col("bad_flag") === 1
            , 1).
            otherwise(0)))
      //enrich with rup entities
      .withColumn("individual_flag", when( col("bankaccount_dda") > 0 ||
      col("govid_ssn") > 0 || col("prin") > 0, 1).otherwise(0))
      .withColumn("individual_bad_flag",
        when( col("bad_flag") === 1 &&
          col("individual_flag") === 1, 1).otherwise(0))
      .withColumn("household_flag", when(
        col("prin_lastname") === 0 &&
          ((col("bankaccount_dda") > 0 ||
            ( col("address_home") > 0 ) &&
              (col("phone_home") > 0 || col("phone_business") > 0 ||
                col("phone_home") > 0 || col("IP") > 0 ))), 1).otherwise(0)).
      withColumn("household_bad_flag",
        when( col("bad_flag") === 1 &&
          col("household_flag") === 1, 1).otherwise(0)).
      withColumn("business_flag", when(col("govid_fein") > 0 ||
        (col("bname_dba") > 0 && col("bname_legal") > 0) || col("address_business") > 0 || col("url") > 0, 1).otherwise(0) ).
      withColumn("business_bad_flag",
        when( col("bad_flag") === 1 &&
          col("business_flag") === 1, 1).otherwise(0)).

      withColumn("velocity_th1_bad_flag", when( col("velocity_th1_flag")>0 && col("bad_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th2_bad_flag", when(col("velocity_th2_flag")>0 && col("bad_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th1_fraud_flag", when(col("velocity_th1_flag")>0 && col("fraud_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th2_fraud_flag", when(col("velocity_th2_flag")>0 && col("fraud_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th1_decline_flag", when( col("velocity_th1_flag")>0 && col("decline_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th2_decline_flag", when(col("velocity_th2_flag")>0 && col("decline_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th1_susp1_flag", when( col("velocity_th1_flag")>0 && col("suspicious_links_1_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th2_susp1_flag", when(col("velocity_th2_flag")>0 && col("suspicious_links_1_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th1_susp4_flag", when( col("velocity_th1_flag")>0 && col("suspicious_links_4_flag") === 1, 1).otherwise(0)).
      withColumn("velocity_th2_susp4_flag", when(col("velocity_th2_flag")>0 && col("suspicious_links_4_flag") === 1, 1).otherwise(0)).

      //convert counts to flags
      withColumn("address_business_flag", when(col("address_business") > 0, 1).otherwise(0) ).
      withColumn("address_home_flag", when(col("address_home") > 0, 1).otherwise(0)).
      withColumn("address_mail_flag", when(col("address_mail") > 0, 1).otherwise(0) ).
      withColumn("bankaccount_dda_flag", when(col("bankaccount_dda") > 0, 1).otherwise(0) ).
      withColumn("bname_dba_flag", when(col("bname_dba") > 0, 1).otherwise(0) ).
      withColumn("bname_legal_flag", when(col("bname_legal") > 0, 1).otherwise(0) ).
      withColumn("bname_flag", when(col("bname_dba")+col("bname_legal") > 0, 1).otherwise(0) ).//
      //withColumn("email_home_flag", when(col("email_home") > 0, 1).otherwise(0) ).
      withColumn("email_business_flag", when(col("email_business") > 0, 1).otherwise(0) ).
      withColumn("device_flag", when(col("device") > 0, 1).otherwise(0) ).
      withColumn("govid_fein_flag", when(col("govid_fein") > 0, 1).otherwise(0) ).
      withColumn("govid_ssn_flag", when(col("govid_ssn") > 0, 1).otherwise(0) ).
      withColumn("ip_flag", when(col("ip") > 0, 1).otherwise(0) ).
      withColumn("phone_business_flag", when(col("phone_business") > 0, 1).otherwise(0) ).
      withColumn("phone_home_flag", when(col("phone_home") > 0, 1).otherwise(0) ).
      withColumn("prin_flag", when(col("prin") > 0, 1).otherwise(0) ).
      withColumn("prin_lastname_flag", when(col("prin_lastname") > 0, 1).otherwise(0) ).
      withColumn("url_flag", when(col("url") > 0, 1).otherwise(0) ).na.fill(0)
      .drop("address_business","address_home","address_mail","bankaccount_dda","bname_dba","bname_legal","email_business","device","govid_fein","govid_ssn","ip","phone_business","phone_home"
      ,"prin","prin_lastname","url")

    // derive merchant level counts
    val sum_aggs = Array(
      "velocity_th1_flag",
      "velocity_th2_flag",
      "link_flag",
      "ipOnly_flag",
      "addressOnly_flag",
      "bad_flag",
      "fraud_flag",
      "fraud_weak_flag",
      "fraud_FRAPP_flag",
      "fraud_1P_flag",
      "fraud_3P_flag",
      "fraud_manu_decline_weak_flag",
      "fraud_manu_decline_flag",
      "decline_weak_flag",
      "decline_flag",
      "credit_flag",
      "credit2_flag",
      "credit_weak_flag",
      "suspicious_links_1_flag",
      "suspicious_links_2_flag",
      "suspicious_links_3_flag",
      "suspicious_links_4_flag",
      "individual_flag",
      "individual_bad_flag",
      "household_flag",
      "household_bad_flag",
      "business_flag",
      "business_bad_flag",
      "velocity_th1_bad_flag",
      "velocity_th2_bad_flag",
      "velocity_th1_fraud_flag",
      "velocity_th2_fraud_flag",
      "velocity_th1_decline_flag",
      "velocity_th2_decline_flag",
      "velocity_th1_susp1_flag",
      "velocity_th2_susp1_flag",
      "velocity_th1_susp4_flag",
      "velocity_th2_susp4_flag",
      "address_business_flag",
      "address_home_flag",
      "address_mail_flag",
      "bankaccount_dda_flag",
      "bname_dba_flag",
      "bname_legal_flag",
      "bname_flag",
      "email_business_flag",
      "device_flag",
      "govid_fein_flag",
      "govid_ssn_flag",
      "ip_flag",
      "phone_business_flag",
      "phone_home_flag",
      "prin_flag",
      "prin_lastname_flag",
      "url_flag").map(t => sum(col(t)).as(t))

    //apply aggs and fill nulls
    val m1pre = m1m2Final.groupBy( $"seed_id", col(dateTimeColumn)).
      agg(sum_aggs.head, sum_aggs.tail: _*).na.fill(0)

    //rename from "flag" to "ct"
    val m1 = m1pre
      .columns.foldLeft(m1pre)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("_flag", "_ct")))//rename flags to counts post aggs
      .withColumnRenamed("seed_id",merchantKey)//add back merch id
      .join(df.select(merchantKey,dateTimeColumn),Seq(merchantKey,dateTimeColumn),"right").na.fill(0)//join to df and convert missing links to 0 links

    //add partition columns
    val dfOut = m1.
      withColumn("year", date_format(col(dateTimeColumn), "yyyy").cast("int")).
      withColumn("month", date_format(col(dateTimeColumn), "MM").cast("int")).
      withColumn("day", date_format(col(dateTimeColumn), "dd").cast("int")).
      withColumnRenamed("seed_id", merchantKey)

    dfOut

  } //end run definition
} // end object
