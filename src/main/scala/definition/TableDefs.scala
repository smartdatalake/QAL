package definition

import java.io.File

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

object TableDefs {

  def load_tpch_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    // TPC-H tables
    // ============================================================> NATION
    val nationSchema = StructType(
      StructField("n_nationkey", LongType, true) ::
        StructField("n_name", StringType, true) ::
        StructField("n_regionkey", LongType, true) ::
        StructField("n_comment", StringType, true) :: Nil)
    val nation = sparkSession.read.parquet(DATA_DIR + "nation.parquet");
    sparkSession.sqlContext.createDataFrame(nation.rdd, nationSchema).createOrReplaceTempView("nation");
    // ============================================================> CUSTOMER
    val customerSchema = StructType(
      StructField("c_custkey", LongType, true) ::
        StructField("c_name", StringType, true) ::
        StructField("c_address", StringType, true) ::
        StructField("c_nationkey", IntegerType, true) ::
        StructField("c_phone", StringType, true) ::
        StructField("c_acctbal", DecimalType(12, 2), true) ::
        StructField("c_mktsegment", StringType, true) ::
        StructField("c_comment", StringType, true) :: Nil);
    val customer = sparkSession.read.parquet(DATA_DIR + "customer.parquet");
    sparkSession.sqlContext.createDataFrame(customer.rdd, customerSchema).createOrReplaceTempView("customer");
    // ============================================================> LINEITEM
    val lineitemSampledSchema = StructType(
      StructField("l_orderkey", LongType, true) :: // 0
        StructField("l_partkey", LongType, true) :: // 1
        StructField("l_suppkey", LongType, true) :: // 2
        StructField("l_linenumber", IntegerType, true) :: // 3
        StructField("l_quantity", DecimalType(12, 2), true) :: // 4
        StructField("l_extendedprice", DecimalType(12, 2), true) :: // 5
        StructField("l_discount", DecimalType(12, 2), true) :: // 6
        StructField("l_tax", DecimalType(12, 2), true) :: // 7
        StructField("l_returnflag", StringType, true) :: // 8
        StructField("l_linestatus", StringType, true) :: // 9
        StructField("l_shipdate", DateType, true) :: // 10
        StructField("l_commitdate", DateType, true) :: // 11
        StructField("l_receiptdate", DateType, true) :: // 12
        StructField("l_shipinstruct", StringType, true) :: // 13
        StructField("l_shipmode", StringType, true) :: // 14
        StructField("l_comment", StringType, true) :: // 15
        StructField("lsratio", DoubleType, true) :: Nil); // 16
    val lineitem = sparkSession.read.parquet(DATA_DIR + "lineitem.parquet");
    sparkSession.sqlContext.createDataFrame(lineitem.rdd, lineitemSampledSchema).createOrReplaceTempView("lineitem");
    // ============================================================> ORDER
    val orderSchema = StructType(
      StructField("o_orderkey", LongType, true) ::
        StructField("o_custkey", LongType, true) ::
        StructField("o_orderstatus", StringType, true) ::
        StructField("o_totalprice", DecimalType(12, 2), true) ::
        StructField("o_orderdate", DateType, true) ::
        StructField("o_orderpriority", StringType, true) ::
        StructField("o_clerk", StringType, true) ::
        StructField("o_shippriority", IntegerType, true) ::
        StructField("o_comment", StringType, true) :: Nil)
    val order = sparkSession.read.parquet(DATA_DIR + "order.parquet");
    sparkSession.sqlContext.createDataFrame(order.rdd, orderSchema).createOrReplaceTempView("orders");
    // ============================================================> PART
    val partSchema = StructType(
      StructField("p_partkey", LongType, true) ::
        StructField("p_name", StringType, true) ::
        StructField("p_mfgr", StringType, true) ::
        StructField("p_brand", StringType, true) ::
        StructField("p_type", StringType, true) ::
        StructField("p_size", IntegerType, true) ::
        StructField("p_container", StringType, true) ::
        StructField("p_retailprice", DecimalType(12, 2), true) ::
        StructField("p_comment", StringType, true) :: Nil)
    val part = sparkSession.read.parquet(DATA_DIR + "part.parquet");
    sparkSession.sqlContext.createDataFrame(part.rdd, partSchema).createOrReplaceTempView("part")
    // ============================================================> PARTSUPP
    val partsuppSchema = StructType(
      StructField("ps_partkey", LongType, true) ::
        StructField("ps_suppkey", LongType, true) ::
        StructField("ps_availqty", IntegerType, true) ::
        StructField("ps_supplycost", DecimalType(12, 2), true) ::
        StructField("ps_comment", StringType, true) :: Nil)
    val partsupp = sparkSession.read.parquet(DATA_DIR + "partsupp.parquet");
    sparkSession.sqlContext.createDataFrame(partsupp.rdd, partsuppSchema).createOrReplaceTempView("partsupp");
    // ============================================================> REGION
    val regionSchema = StructType(
      StructField("r_regionkey", LongType, true) ::
        StructField("r_name", StringType, true) ::
        StructField("r_comment", StringType, true) :: Nil)
    val region = sparkSession.read.parquet(DATA_DIR + "region.parquet");
    sparkSession.sqlContext.createDataFrame(region.rdd, regionSchema).createOrReplaceTempView("region");
    // ============================================================> SUPPLIER
    val supplierSchema = StructType(
      StructField("s_suppkey", LongType, true) ::
        StructField("s_name", StringType, true) ::
        StructField("s_address", StringType, true) ::
        StructField("s_nationkey", LongType, true) ::
        StructField("s_phone", StringType, true) ::
        StructField("s_acctbal", DecimalType(12, 2), true) ::
        StructField("s_comment", StringType, true) :: Nil)

    val supplier = sparkSession.read.parquet(DATA_DIR + "supplier.parquet");
    sparkSession.sqlContext.createDataFrame(supplier.rdd, supplierSchema).createOrReplaceTempView("supplier");
  }

  val tpch_queries_nation = "q2" :: "q5" :: "q7" :: "q8" :: "q9" :: "q10" :: "q11" :: "q20" :: "q21" :: Nil;
  val tpch_queries_customer = "q3" :: "q5" :: "q7" :: "q8" :: "q10" :: "q13" :: "q18" :: "q22" :: "q32" :: Nil;
  val tpch_queries_lineitem = "q1" :: "q3" :: "q4" :: "q5" :: "q6" :: "q7" :: "q8" :: "q9" :: "q10" :: "q12" :: "q14" :: "q15" :: "q17" :: "q18" :: "q19" :: "q20" :: "q21" :: "q32" :: Nil;
  val tpch_queries_order = "q3" :: "q4" :: "q5" :: "q7" :: "q8" :: "q9" :: "q10" :: "q12" :: "q13" :: "q18" :: "q21" :: "q22" :: "q32" :: Nil;
  val tpch_queries_part = "q2" :: "q8" :: "q9" :: "q14" :: "q16" :: "q17" :: "q19" :: "q20" :: Nil;
  val tpch_queries_partsupp = "q2" :: "q9" :: "q11" :: "q16" :: "q20" :: Nil;
  val tpch_queries_region = "q2" :: "q5" :: "q8" :: Nil;
  val tpch_queries_supplier = "q2" :: "q5" :: "q7" :: "q8" :: "q9" :: "q11" :: "q15" :: "q16" :: "q20" :: "q21" :: Nil;

  val tpcds_queries_storesales = "q3" :: "q3_error" :: "q6" :: "q7" :: "q8" :: "q9" :: "q13" :: "q17" :: "q19" :: "q23" :: "q24" :: "q25" :: "q27" :: "q29" :: "q33" :: "q34" :: "q36" :: "q42" :: "q43" :: "q46" :: "q50" :: "q52" :: "q53" :: "q55" :: "q56" :: "q63" :: "q65" :: "q67" :: "q68" :: "q70" :: "q73" :: Nil;
  val tpcds_queries_catalogsales = "q15" :: "q16" :: "q17" :: "q20" :: "q23" :: "q25" :: "q26" :: "q29" :: "q33" :: "q32" :: "q37" :: "q56" :: Nil;
  val tpcds_queries_storereturns = "q17" :: "q24" :: "q25" :: "q29" :: "q50" :: Nil;
  val tpcds_queries_datedim = "q3" :: "q3_error" :: "q6" :: "q7" :: "q8" :: "q12" :: "q13" :: "q15" :: "q16" :: "q17" :: "q19" :: "q20" :: "q23" :: "q25" :: "q26" :: "q27" :: "q29" :: "q32" :: "q33" :: "q34" :: "q36" :: "q37" :: "q42" :: "q43" :: "q46" :: "q50" :: "q52" :: "q53" :: "q55" :: "q56" :: "q63" :: "q65" :: "q67" :: "q68" :: "q70" :: "q73" :: Nil;
  val tpcds_queries_item = "q3" :: "q3_error" :: "q6" :: "q7" :: "q12" :: "q17" :: "q19" :: "q20" :: "q23" :: "q24" :: "q25" :: "q26" :: "q27" :: "q29" :: "q32" :: "q33" :: "q36" :: "q37" :: "q42" :: "q52" :: "q53" :: "q55" :: "q56" :: "q63" :: "q65" :: "q67" :: Nil;
  val tpcds_queries_callcenter = "q16" :: Nil;
  val tpcds_queries_catalogreturns = "q16" :: Nil;
  val tpcds_queries_customeraddress = "q6" :: "q8" :: "q13" :: "q15" :: "q16" :: "q19" :: "q24" :: "q33" :: "q46" :: "q56" :: "q68" :: Nil;
  val tpcds_queries_customer = "q6" :: "q8" :: "q15" :: "q19" :: "q23" :: "q24" :: "q34" :: "q46" :: "q68" :: "q73" :: Nil;
  val tpcds_queries_customerdemographics = "q7" :: "q13" :: "q26" :: "q27" :: Nil;
  val tpcds_queries_householddemographics = "q13" :: "q34" :: "q46" :: "q68" :: "q73" :: Nil;
  val tpcds_queries_website = {
    ""
  };
  val tpcds_queries_warehouse = {
    ""
  };
  val tpcds_queries_inventory = "q37" :: Nil;
  val tpcds_queries_promotion = "q7" :: "q26" :: Nil;
  val tpcds_queries_reason = "q9" :: Nil;
  val tpcds_queries_store = "q8" :: "q13" :: "q17" :: "q19" :: "q24" :: "q25" :: "q27" :: "q29" :: "q34" :: "q36" :: "q43" :: "q46" :: "q50" :: "q53" :: "q63" :: "q65" :: "q67" :: "q68" :: "q70" :: "q73" :: Nil;
  val tpcds_queries_timedim = {
    ""
  };
  val tpcds_queries_websales = "q12" :: "q23" :: "q33" :: "q56" :: Nil;
  val tpcds_queries_webreturns = {
    ""
  };
  val tpcds_queries_web_page = {
    ""
  };
  val tpcds_queries_catalogpage = {
    ""
  };
  val tpcds_queries_shipmode = {
    ""
  };

  val insta_queries_orders = "q1" :: "q5" :: "q11" :: Nil;
  val insta_queries_products = "q2" :: "q3" :: "q4" :: "q6" :: "q7" :: "q8" :: "q12" :: "q13" :: "q14" :: Nil;
  val insta_queries_aisles = "q4" :: "q8" :: "q14" :: Nil;
  val insta_queries_departements = "q3" :: "q7" :: "q13" :: Nil;
  val insta_queries_orderproducts = "q1" :: "q2" :: "q3" :: "q4" :: "q5" :: "q6" :: "q7" :: "q8" :: "q11" :: "q12" :: "q13" :: "q14" :: Nil;

  def load_instacart_tables(sparkSession: SparkSession, DATA_DIR: String, SAMPLE_DIR: String, RESULTS_DIR: String, query: String, percentage: String, budget: String) = {

    // INSTACART tables

    // ============================================================> ORDERS
    if (insta_queries_orders.contains(query)) {
      val ordersSchema = StructType(
        StructField("o_order_id", LongType, true) ::
          StructField("o_user_id", LongType, true) ::
          StructField("o_eval_set", StringType, true) ::
          StructField("o_order_number", LongType, true) ::
          StructField("o_order_dow", LongType, true) ::
          StructField("o_order_hour_of_day", LongType, true) ::
          StructField("o_day_since_prior", DecimalType(12, 2), true) :: Nil)
      if (percentage.equals("")) {
        val orders = sparkSession.read.parquet(DATA_DIR + "orders.parquet");
        //        val nation = sparkSession.sqlContext.read.format("csv").schema(nationSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "nation.tbl");
        sparkSession.sqlContext.createDataFrame(orders.rdd, ordersSchema).createOrReplaceTempView("orders");
      } else {
        val orders = sparkSession.read.parquet(DATA_DIR + "orders.parquet");
        sparkSession.sqlContext.createDataFrame(orders.rdd, ordersSchema).createOrReplaceTempView("orders");
      }
    }

    // ============================================================> PRODUCTS
    if (insta_queries_products.contains(query)) {
      val productsSchema = StructType(
        StructField("p_product_id", LongType, true) ::
          StructField("p_product_name", StringType, true) ::
          StructField("p_aisle_id", LongType, true) ::
          StructField("p_department_id", LongType, true) :: Nil)
      if (percentage.equals("")) {
        val products = sparkSession.read.parquet(DATA_DIR + "products.parquet");
        //        val nation = sparkSession.sqlContext.read.format("csv").schema(nationSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "nation.tbl");
        sparkSession.sqlContext.createDataFrame(products.rdd, productsSchema).createOrReplaceTempView("products");
      } else {
        val products = sparkSession.read.parquet(DATA_DIR + "products.parquet");
        sparkSession.sqlContext.createDataFrame(products.rdd, productsSchema).createOrReplaceTempView("products");
      }
    }

    // ============================================================> AISLES
    if (insta_queries_aisles.contains(query)) {
      val aislesSchema = StructType(
        StructField("a_aisle_id", LongType, true) ::
          StructField("a_aisle_name", StringType, true) :: Nil)
      if (percentage.equals("")) {
        val aisles = sparkSession.read.parquet(DATA_DIR + "aisles.parquet");
        //        val nation = sparkSession.sqlContext.read.format("csv").schema(nationSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "nation.tbl");
        sparkSession.sqlContext.createDataFrame(aisles.rdd, aislesSchema).createOrReplaceTempView("aisles");
      } else {
        val aisles = sparkSession.read.parquet(DATA_DIR + "aisles.parquet");
        sparkSession.sqlContext.createDataFrame(aisles.rdd, aislesSchema).createOrReplaceTempView("aisles");
      }
    }

    // ============================================================> DEPARTEMENTS
    if (insta_queries_departements.contains(query)) {
      val departmentsSchema = StructType(
        StructField("d_department_id", LongType, true) ::
          StructField("d_department", StringType, true) :: Nil)
      if (percentage.equals("")) {
        val departments = sparkSession.read.parquet(DATA_DIR + "departments.parquet");
        //        val nation = sparkSession.sqlContext.read.format("csv").schema(nationSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "nation.tbl");
        sparkSession.sqlContext.createDataFrame(departments.rdd, departmentsSchema).createOrReplaceTempView("departments");
      } else {
        val departments = sparkSession.read.parquet(DATA_DIR + "departments.parquet");
        sparkSession.sqlContext.createDataFrame(departments.rdd, departmentsSchema).createOrReplaceTempView("departments");
      }
    }

    // ============================================================> ORDERPRODUCTS
    if (insta_queries_orderproducts.contains(query)) {
      val orderproductsSchema = StructType(
        StructField("op_order_id", LongType, true) ::
          StructField("op_product_id", LongType, true) ::
          StructField("op_add_to_cart_order", LongType, true) ::
          StructField("op_reordered", LongType, true) :: Nil)

      val SketchSchema_OP_ORDER_ID_COUNT = StructType(
        StructField("op_order_id", LongType, true) :: // 0
          StructField("count", LongType, true) :: Nil); // 1

      val SketchSchema_OP_PRODUCT_ID_COUNT = StructType(
        StructField("op_product_id", LongType, true) :: // 0
          StructField("count", LongType, true) :: Nil); // 1

      if (percentage.equals("")) {
        val orderproducts = sparkSession.read.parquet(DATA_DIR + "orderproducts.parquet");
        //        val nation = sparkSession.sqlContext.read.format("csv").schema(nationSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "nation.tbl");
        sparkSession.sqlContext.createDataFrame(orderproducts.rdd, orderproductsSchema).createOrReplaceTempView("orderproducts");

        val orderproducts1_Sketch = sparkSession.read.parquet(SAMPLE_DIR + "orderproducts_sketch_op_order_id-count.parquet");
        sparkSession.sqlContext.createDataFrame(orderproducts1_Sketch.rdd, SketchSchema_OP_ORDER_ID_COUNT).createOrReplaceTempView("orderproducts_orderid_Sketch");

        val orderproducts2_Sketch = sparkSession.read.parquet(SAMPLE_DIR + "orderproducts_sketch_op_product_id-count.parquet");
        sparkSession.sqlContext.createDataFrame(orderproducts2_Sketch.rdd, SketchSchema_OP_PRODUCT_ID_COUNT).createOrReplaceTempView("orderproducts_productid_Sketch");
      } else {

        if (query.equals("q1") ||
          query.equals("q6") ||
          query.equals("q7") ||
          query.equals("q8")) { //sample 4
          print("op_order_id\t");
          val orderproducts = sparkSession.read.parquet(SAMPLE_DIR + "orderproducts_strat_op_order_id-count.parquet");
          sparkSession.sqlContext.createDataFrame(orderproducts.rdd, orderproductsSchema).createOrReplaceTempView("orderproducts");
        } else if (query.equals("q2") ||
          query.equals("q3") ||
          query.equals("q4") ||
          query.equals("q5")) {
          print("op_product_id\t");
          val orderproducts = sparkSession.read.parquet(SAMPLE_DIR + "orderproducts_strat_op_product_id-count.parquet");
          sparkSession.sqlContext.createDataFrame(orderproducts.rdd, orderproductsSchema).createOrReplaceTempView("orderproducts");
        } else { // no sampling
          print("none\t");
          val orderproducts = sparkSession.read.parquet(DATA_DIR + "orderproducts.parquet");
          sparkSession.sqlContext.createDataFrame(orderproducts.rdd, orderproductsSchema).createOrReplaceTempView("orderproducts");
        }
      }
    }
  }

  def load_tpcds_tables(sparkSession: SparkSession, LOCAL_PARENT_DIR: String, LOCAL_SAMPLE_DIR: String, LOCAL_RESULTS_DIR: String, oldQueryName: String, percentage: String) = {

    // TPC-DS tables

    var query = oldQueryName;
    if (query.contains("_")) {
      val queryOptions = oldQueryName.split("_");
      query = queryOptions(0);
    }
    println(query);

    // ============================================================> STORE_SALES
    if (tpcds_queries_storesales.contains(query)) {
      val storesalesSchema = StructType(
        StructField("ss_sold_time_sk", IntegerType, true) :: //0
          StructField("ss_item_sk", IntegerType, true) :: //1
          StructField("ss_customer_sk", IntegerType, true) :: //2
          StructField("ss_cdemo_sk", IntegerType, true) :: //3
          StructField("ss_hdemo_sk", IntegerType, true) :: //4
          StructField("ss_addr_sk", IntegerType, true) :: //5
          StructField("ss_store_sk", IntegerType, true) :: //6
          StructField("ss_promo_sk", IntegerType, true) :: //7
          StructField("ss_ticket_number", IntegerType, true) :: //8
          StructField("ss_quantity", IntegerType, true) :: //9
          StructField("ss_wholesale_cost", DecimalType(7, 2), true) :: //10
          StructField("ss_list_price", DecimalType(7, 2), true) :: //11
          StructField("ss_sales_price", DecimalType(7, 2), true) :: //12
          StructField("ss_ext_discount_amt", DecimalType(7, 2), true) :: //13
          StructField("ss_ext_sales_price", DecimalType(7, 2), true) :: //14
          StructField("ss_ext_wholesale_cost", DecimalType(7, 2), true) :: //15
          StructField("ss_ext_list_price", DecimalType(7, 2), true) :: //16
          StructField("ss_ext_tax", DecimalType(7, 2), true) :: //17
          StructField("ss_coupon_amt", DecimalType(7, 2), true) :: //18
          StructField("ss_net_paid", DecimalType(7, 2), true) :: //19
          StructField("ss_net_paid_inc_tax", DecimalType(7, 2), true) :: //20
          StructField("ss_net_profit", DecimalType(7, 2), true) :: //21
          StructField("ss_sold_date_sk", IntegerType, true) :: Nil) //22
      if (percentage.equals("")) {
        val store_sales = sparkSession.read.parquet(LOCAL_PARENT_DIR + "store_sales.parquet");
        sparkSession.sqlContext.createDataFrame(store_sales.rdd, storesalesSchema).createOrReplaceTempView("store_sales");

        //        val store_sales_join_dd = sparkSession.read.parquet(LOCAL_SAMPLE_DIR + "ssjoindd_inter_ss_join_dd.parquet");
        //        store_sales_join_dd.createOrReplaceTempView("ss_j_dd");
        //            store_sales.registerTempTable("store_sales");
      } else {
        val store_sales = sparkSession.read.parquet(LOCAL_SAMPLE_DIR + "ss_strat_" + percentage + "_" + query + ".parquet");
        sparkSession.sqlContext.createDataFrame(store_sales.rdd, store_sales.schema).createOrReplaceTempView("store_sales");
        println(store_sales.schema);
      }
    }

    // ============================================================> CATALOG SALES
    if (tpcds_queries_catalogsales.contains(query)) {
      val catalogsalesSchema = StructType(
        StructField("cs_sold_time_sk", IntegerType, true) :: //0
          StructField("cs_ship_date_sk", IntegerType, true) :: //1
          StructField("cs_bill_customer_sk", IntegerType, true) :: //2
          StructField("cs_bill_cdemo_sk", IntegerType, true) :: //3
          StructField("cs_bill_hdemo_sk", IntegerType, true) :: //4
          StructField("cs_bill_addr_sk", IntegerType, true) :: //5
          StructField("cs_ship_customer_sk", IntegerType, true) :: //6
          StructField("cs_ship_cdemo_sk", IntegerType, true) :: //7
          StructField("cs_ship_hdemo_sk", IntegerType, true) :: //8
          StructField("cs_ship_addr_sk", IntegerType, true) :: //9
          StructField("cs_call_center_sk", IntegerType, true) :: //10
          StructField("cs_catalog_page_sk", IntegerType, true) :: //11
          StructField("cs_ship_mode_sk", IntegerType, true) :: //12
          StructField("cs_warehouse_sk", IntegerType, true) :: //13
          StructField("cs_item_sk", IntegerType, true) :: //14
          StructField("cs_promo_sk", IntegerType, true) :: //15
          StructField("cs_order_number", IntegerType, true) :: //16
          StructField("cs_quantity", IntegerType, true) :: //17
          StructField("cs_wholesale_cost", DecimalType(7, 2), true) :: //18
          StructField("cs_list_price", DecimalType(7, 2), true) :: //19
          StructField("cs_sales_price", DecimalType(7, 2), true) :: //20
          StructField("cs_ext_discount_amt", DecimalType(7, 2), true) :: //21
          StructField("cs_ext_sales_price", DecimalType(7, 2), true) :: //22
          StructField("cs_ext_wholesale_cost", DecimalType(7, 2), true) :: //23
          StructField("cs_ext_list_price", DecimalType(7, 2), true) :: //24
          StructField("cs_ext_tax", DecimalType(7, 2), true) :: //25
          StructField("cs_coupon_amt", DecimalType(7, 2), true) :: //26
          StructField("cs_ext_ship_cost", DecimalType(7, 2), true) :: //27
          StructField("cs_net_paid", DecimalType(7, 2), true) :: //28
          StructField("cs_net_paid_inc_tax", DecimalType(7, 2), true) :: //29
          StructField("cs_net_paid_inc_ship", DecimalType(7, 2), true) :: //30
          StructField("cs_net_paid_inc_ship_tax", DecimalType(7, 2), true) :: //31
          StructField("cs_net_profit", DecimalType(7, 2), true) :: //32
          StructField("cs_sold_date_sk", IntegerType, true) :: Nil); //33
      if (!percentage.contains("") && (query.equals("q15") || query.equals("q16") || query.equals("q18") || query.equals("q20") || query.equals("q23") || query.equals("q25") || query.equals("q26") || query.equals("q29") || query.equals("q32") || query.equals("q37"))) {
        val catalog_sales = sparkSession.read.parquet(LOCAL_SAMPLE_DIR + "cs_strat_" + percentage + "_" + query + ".parquet");
        sparkSession.sqlContext.createDataFrame(catalog_sales.rdd, catalogsalesSchema).registerTempTable("catalog_sales");
      } else {
        val catalog_sales = sparkSession.read.parquet(LOCAL_PARENT_DIR + "catalog_sales.parquet");
        sparkSession.sqlContext.createDataFrame(catalog_sales.rdd, catalogsalesSchema).registerTempTable("catalog_sales");
        //    catalog_sales.registerTempTable("catalog_sales");
      }
    }

    // ============================================================> STORE RETURNS
    if (tpcds_queries_storereturns.contains(query)) {
      val storereturnsSchema = StructType(
        StructField("sr_return_time_sk", LongType, true) :: //0
          StructField("sr_item_sk", LongType, true) :: //1
          StructField("sr_customer_sk", LongType, true) :: //2
          StructField("sr_cdemo_sk", LongType, true) :: //3
          StructField("sr_hdemo_sk", LongType, true) :: //4
          StructField("sr_addr_sk", LongType, true) :: //5
          StructField("sr_store_sk", LongType, true) :: //6
          StructField("sr_reason_sk", LongType, true) :: //7
          StructField("sr_ticket_number", LongType, true) :: //8
          StructField("sr_return_quantity", LongType, true) :: //9
          StructField("sr_return_amt", DecimalType(7, 2), true) :: //10
          StructField("sr_return_tax", DecimalType(7, 2), true) :: //11
          StructField("sr_return_amt_inc_tax", DecimalType(7, 2), true) :: //12
          StructField("sr_fee", DecimalType(7, 2), true) :: //13
          StructField("sr_return_ship_cost", DecimalType(7, 2), true) :: //14
          StructField("sr_refunded_cash", DecimalType(7, 2), true) :: //15
          StructField("sr_reversed_charge", DecimalType(7, 2), true) :: //16
          StructField("sr_store_credit", DecimalType(7, 2), true) :: //17
          StructField("sr_net_loss", DecimalType(7, 2), true) :: //18
          StructField("sr_returned_date_sk", IntegerType, true) :: Nil); //19
      if (!percentage.contains("") && (query.equals("q50") || query.equals("q24") || query.equals("q25"))) {
        val store_returns = sparkSession.read.parquet(LOCAL_SAMPLE_DIR + "sr_strat_" + percentage + "_" + query + ".parquet");
        sparkSession.sqlContext.createDataFrame(store_returns.rdd, storereturnsSchema).registerTempTable("store_returns");
      } else {
        val store_returns = sparkSession.read.parquet(LOCAL_PARENT_DIR + "store_returns.parquet");
        sparkSession.sqlContext.createDataFrame(store_returns.rdd, storereturnsSchema).registerTempTable("store_returns");
        //    store_returns.registerTempTable("store_returns");
      }
    }

    // ============================================================> DATE DIM
    if (tpcds_queries_datedim.contains(query)) {
      val datedimSchema = StructType(
        StructField("d_date_sk", IntegerType, true) ::
          StructField("d_date_id", StringType, true) ::
          StructField("d_date", StringType, true) ::
          StructField("d_month_seq", IntegerType, true) ::
          StructField("d_week_seq", IntegerType, true) ::
          StructField("d_quarter_seq", IntegerType, true) ::
          StructField("d_year", IntegerType, true) ::
          StructField("d_dow", IntegerType, true) ::
          StructField("d_moy", IntegerType, true) ::
          StructField("d_dom", IntegerType, true) ::
          StructField("d_qoy", IntegerType, true) ::
          StructField("d_fy_year", IntegerType, true) ::
          StructField("d_fy_quarter_seq", IntegerType, true) ::
          StructField("d_fy_week_seq", IntegerType, true) ::
          StructField("d_day_name", StringType, true) ::
          StructField("d_quarter_name", StringType, true) ::
          StructField("d_holiday", StringType, true) ::
          StructField("d_weekend", StringType, true) ::
          StructField("d_following_holiday", StringType, true) ::
          StructField("d_first_dom", IntegerType, true) ::
          StructField("d_last_dom", IntegerType, true) ::
          StructField("d_same_day_ly", IntegerType, true) ::
          StructField("d_same_day_lq", IntegerType, true) ::
          StructField("d_current_day", StringType, true) ::
          StructField("d_current_week", StringType, true) ::
          StructField("d_current_month", StringType, true) ::
          StructField("d_current_quarter", StringType, true) ::
          StructField("d_current_year", StringType, true) :: Nil);
      val date_dim = sparkSession.read.parquet(LOCAL_PARENT_DIR + "date_dim.parquet");
      sparkSession.sqlContext.createDataFrame(date_dim.rdd, datedimSchema).createOrReplaceTempView("date_dim");
      //    date_dim.registerTempTable("date_dim");
    }

    // ============================================================> ITEM
    if (tpcds_queries_item.contains(query)) {
      val itemSchema = StructType(
        StructField("i_item_sk", IntegerType, true) ::
          StructField("i_item_id", StringType, true) ::
          StructField("i_rec_start_date", StringType, true) ::
          StructField("i_rec_end_date", StringType, true) ::
          StructField("i_item_desc", StringType, true) ::
          StructField("i_current_price", DecimalType(7, 2), true) ::
          StructField("i_wholesale_cost", DecimalType(7, 2), true) ::
          StructField("i_brand_id", IntegerType, true) ::
          StructField("i_brand", StringType, true) ::
          StructField("i_class_id", IntegerType, true) ::
          StructField("i_class", StringType, true) ::
          StructField("i_category_id", IntegerType, true) ::
          StructField("i_category", StringType, true) ::
          StructField("i_manufact_id", IntegerType, true) ::
          StructField("i_manufact", StringType, true) ::
          StructField("i_size", StringType, true) ::
          StructField("i_formulation", StringType, true) ::
          StructField("i_color", StringType, true) ::
          StructField("i_units", StringType, true) ::
          StructField("i_container", StringType, true) ::
          StructField("i_manager_id", IntegerType, true) ::
          StructField("i_product_name", StringType, true) :: Nil);
      val item = sparkSession.read.parquet(LOCAL_PARENT_DIR + "item.parquet");
      sparkSession.sqlContext.createDataFrame(item.rdd, itemSchema).createOrReplaceTempView("item");
      //    item.registerTempTable("item");
    }

    // ============================================================> CALL CENTER
    if (tpcds_queries_callcenter.contains(query)) {
      val callcenterSchema = StructType(
        StructField("cc_call_center_sk", IntegerType, true) ::
          StructField("cc_call_center_id", StringType, true) ::
          StructField("cc_rec_start_date", DateType, true) ::
          StructField("cc_rec_end_date", DateType, true) ::
          StructField("cc_closed_date_sk", IntegerType, true) ::
          StructField("cc_open_date_sk", IntegerType, true) ::
          StructField("cc_name", StringType, true) ::
          StructField("cc_class", StringType, true) ::
          StructField("cc_employees", IntegerType, true) ::
          StructField("cc_sq_ft", IntegerType, true) ::
          StructField("cc_hours", StringType, true) ::
          StructField("cc_manager", StringType, true) ::
          StructField("cc_mkt_id", IntegerType, true) ::
          StructField("cc_mkt_class", StringType, true) ::
          StructField("cc_mkt_desc", StringType, true) ::
          StructField("cc_market_manager", StringType, true) ::
          StructField("cc_division", IntegerType, true) ::
          StructField("cc_division_name", StringType, true) ::
          StructField("cc_company", IntegerType, true) ::
          StructField("cc_company_name", StringType, true) ::
          StructField("cc_street_number", StringType, true) ::
          StructField("cc_street_name", StringType, true) ::
          StructField("cc_street_type", StringType, true) ::
          StructField("cc_suite_number", StringType, true) ::
          StructField("cc_city", StringType, true) ::
          StructField("cc_county", StringType, true) ::
          StructField("cc_state", StringType, true) ::
          StructField("cc_zip", StringType, true) ::
          StructField("cc_country", StringType, true) ::
          StructField("cc_gmt_offset", DecimalType(5, 2), true) ::
          StructField("cc_tax_percentage", DecimalType(5, 2), true) :: Nil)
      val call_center = sparkSession.read.parquet(LOCAL_PARENT_DIR + "call_center.parquet");
      sparkSession.sqlContext.createDataFrame(call_center.rdd, callcenterSchema).registerTempTable("call_center");
      //    call_center.registerTempTable("call_center");
    }

    // ============================================================> CATALOG RETURNS
    if (tpcds_queries_catalogreturns.contains(query)) {
      val catalogreturnsSchema = StructType(
        StructField("cr_returned_time_sk", IntegerType, true) ::
          StructField("cr_item_sk", IntegerType, true) ::
          StructField("cr_refunded_customer_sk", IntegerType, true) ::
          StructField("cr_refunded_cdemo_sk", IntegerType, true) ::
          StructField("cr_refunded_hdemo_sk", IntegerType, true) ::
          StructField("cr_refunded_addr_sk", IntegerType, true) ::
          StructField("cr_returning_customer_sk", IntegerType, true) ::
          StructField("cr_returning_cdemo_sk", IntegerType, true) ::
          StructField("cr_returning_hdemo_sk", IntegerType, true) ::
          StructField("cr_returning_addr_sk", IntegerType, true) ::
          StructField("cr_call_center_sk", IntegerType, true) ::
          StructField("cr_catalog_page_sk", IntegerType, true) ::
          StructField("cr_ship_mode_sk", IntegerType, true) ::
          StructField("cr_warehouse_sk", IntegerType, true) ::
          StructField("cr_reason_sk", IntegerType, true) ::
          StructField("cr_order_number", IntegerType, true) ::
          StructField("cr_return_quantity", IntegerType, true) ::
          StructField("cr_return_amount", DecimalType(7, 2), true) ::
          StructField("cr_return_tax", DecimalType(7, 2), true) ::
          StructField("cr_return_amt_inc_tax", DecimalType(7, 2), true) ::
          StructField("cr_fee", DecimalType(7, 2), true) ::
          StructField("cr_return_ship_cost", DecimalType(7, 2), true) ::
          StructField("cr_refunded_cash", DecimalType(7, 2), true) ::
          StructField("cr_reversed_charge", DecimalType(7, 2), true) ::
          StructField("cr_store_credit", DecimalType(7, 2), true) ::
          StructField("cr_net_loss", DecimalType(7, 2), true) ::
          StructField("cr_returned_date_sk", IntegerType, true) :: Nil)
      val catalog_returns = sparkSession.read.parquet(LOCAL_PARENT_DIR + "catalog_returns.parquet");
      sparkSession.sqlContext.createDataFrame(catalog_returns.rdd, catalogreturnsSchema).registerTempTable("catalog_returns");
      //    catalog_returns.registerTempTable("catalog_returns");
    }

    // ============================================================> CUSTOMER ADDRESS
    if (tpcds_queries_customeraddress.contains(query)) {
      val customeraddressSchema = StructType(
        StructField("ca_address_sk", IntegerType, true) ::
          StructField("ca_address_id", StringType, true) ::
          StructField("ca_street_number", StringType, true) ::
          StructField("ca_street_name", StringType, true) ::
          StructField("ca_street_type", StringType, true) ::
          StructField("ca_suite_number", StringType, true) ::
          StructField("ca_city", StringType, true) ::
          StructField("ca_county", StringType, true) ::
          StructField("ca_state", StringType, true) ::
          StructField("ca_zip", StringType, true) ::
          StructField("ca_country", StringType, true) ::
          StructField("ca_gmt_offset", DecimalType(5, 2), true) ::
          StructField("ca_location_type", StringType, true) :: Nil)
      val customer_address = sparkSession.read.parquet(LOCAL_PARENT_DIR + "customer_address.parquet");
      //    customer_address.registerTempTable("customer_address");
      sparkSession.sqlContext.createDataFrame(customer_address.rdd, customeraddressSchema).registerTempTable("customer_address");
    }

    // ============================================================> CUSTOMER
    if (tpcds_queries_customer.contains(query)) {
      val customerSchema = StructType(
        StructField("c_customer_sk", IntegerType, true) ::
          StructField("c_customer_id", StringType, true) ::
          StructField("c_current_cdemo_sk", IntegerType, true) ::
          StructField("c_current_hdemo_sk", IntegerType, true) ::
          StructField("c_current_addr_sk", IntegerType, true) ::
          StructField("c_first_shipto_date_sk", IntegerType, true) ::
          StructField("c_first_sales_date_sk", IntegerType, true) ::
          StructField("c_salutation", StringType, true) ::
          StructField("c_first_name", StringType, true) ::
          StructField("c_last_name", StringType, true) ::
          StructField("c_preferred_cust_flag", StringType, true) ::
          StructField("c_birth_day", IntegerType, true) ::
          StructField("c_birth_month", IntegerType, true) ::
          StructField("c_birth_year", IntegerType, true) ::
          StructField("c_birth_country", StringType, true) ::
          StructField("c_login", StringType, true) ::
          StructField("c_email_address", StringType, true) ::
          StructField("c_last_review_date", StringType, true) :: Nil)
      val customer = sparkSession.read.parquet(LOCAL_PARENT_DIR + "customer.parquet");
      sparkSession.sqlContext.createDataFrame(customer.rdd, customerSchema).registerTempTable("customer");
      //    customer.registerTempTable("customer");
    }

    // ============================================================> CUSTOMER DEMOGRAPHICS
    if (tpcds_queries_customerdemographics.contains(query)) {
      val custmoerdemographicsSchema = StructType(
        StructField("cd_demo_sk", IntegerType, true) ::
          StructField("cd_gender", StringType, true) ::
          StructField("cd_marital_status", StringType, true) ::
          StructField("cd_education_status", StringType, true) ::
          StructField("cd_purchase_estimate", IntegerType, true) ::
          StructField("cd_credit_rating", StringType, true) ::
          StructField("cd_dep_count", IntegerType, true) ::
          StructField("cd_dep_employed_count", IntegerType, true) ::
          StructField("cd_dep_college_count", IntegerType, true) :: Nil)
      val customer_demographics = sparkSession.read.parquet(LOCAL_PARENT_DIR + "customer_demographics.parquet");
      sparkSession.sqlContext.createDataFrame(customer_demographics.rdd, custmoerdemographicsSchema).registerTempTable("customer_demographics");
      //    customer_demographics.registerTempTable("customer_demographics");
    }

    // ============================================================> HOUSEHOLD DEMOGRAPHICS
    if (tpcds_queries_householddemographics.contains(query)) {
      val householddemographicsSchema = StructType(
        StructField("hd_demo_sk", IntegerType, true) ::
          StructField("hd_income_band_sk", IntegerType, true) ::
          StructField("hd_buy_potential", StringType, true) ::
          StructField("hd_dep_count", IntegerType, true) ::
          StructField("hd_vehicle_count", IntegerType, true) :: Nil)
      val household_demographics = sparkSession.read.parquet(LOCAL_PARENT_DIR + "household_demographics.parquet");
      sparkSession.sqlContext.createDataFrame(household_demographics.rdd, householddemographicsSchema).registerTempTable("household_demographics");
      //    household_demographics.registerTempTable("household_demographics");
    }

    // ============================================================> INVENTORY
    if (tpcds_queries_inventory.contains(query)) {
      val inventorySchema = StructType(
        StructField("inv_date_sk", IntegerType, true) ::
          StructField("inv_item_sk", IntegerType, true) ::
          StructField("inv_warehouse_sk", IntegerType, true) ::
          StructField("inv_quantity_on_hand", IntegerType, true) :: Nil)
      val inventory = sparkSession.read.parquet(LOCAL_PARENT_DIR + "inventory.parquet");
      sparkSession.sqlContext.createDataFrame(inventory.rdd, inventorySchema).registerTempTable("inventory");
      //    inventory.registerTempTable("inventory");
    }

    // ============================================================> INCOME BAND
    //    if (queries_incomeband.contains(query)) {
    //    val income_band = sparkSession.read.parquet(LOCAL_PARENT_DIR + "income_band.parquet");
    //    //    income_band.registerTempTable("income_band");
    //    val incomebandSchema = StructType(
    //      StructField("ib_income_band_sk", IntegerType, true) ::
    //        StructField("ib_lower_bound", IntegerType, true) ::
    //        StructField("ib_upper_bound", IntegerType, true) :: Nil)
    //    sparkSession.sqlContext.createDataFrame(income_band.rdd, incomebandSchema).registerTempTable("income_band");
    //  }

    // ============================================================> PROMOTION
    if (tpcds_queries_promotion.contains(query)) {
      val promotionSchema = StructType(
        StructField("p_promo_sk", IntegerType, true) ::
          StructField("p_promo_id", StringType, true) ::
          StructField("p_start_date_sk", IntegerType, true) ::
          StructField("p_end_date_sk", IntegerType, true) ::
          StructField("p_item_sk", IntegerType, true) ::
          StructField("p_cost", DecimalType(15, 2), true) ::
          StructField("p_response_target", IntegerType, true) ::
          StructField("p_promo_name", StringType, true) ::
          StructField("p_channel_dmail", StringType, true) ::
          StructField("p_channel_email", StringType, true) ::
          StructField("p_channel_catalog", StringType, true) ::
          StructField("p_channel_tv", StringType, true) ::
          StructField("p_channel_radio", StringType, true) ::
          StructField("p_channel_press", StringType, true) ::
          StructField("p_channel_event", StringType, true) ::
          StructField("p_channel_demo", StringType, true) ::
          StructField("p_channel_details", StringType, true) ::
          StructField("p_purpose", StringType, true) ::
          StructField("p_discount_active", StringType, true) :: Nil);
      val promotion = sparkSession.read.parquet(LOCAL_PARENT_DIR + "promotion.parquet");
      sparkSession.sqlContext.createDataFrame(promotion.rdd, promotionSchema).registerTempTable("promotion");
      //    promotion.registerTempTable("promotion");
    }

    // ============================================================> REASON
    if (tpcds_queries_reason.contains(query)) {
      val reasonSchema = StructType(
        StructField("r_reason_sk", IntegerType, true) ::
          StructField("r_reason_id", StringType, true) ::
          StructField("r_reason_desc", StringType, true) :: Nil)
      val reason = sparkSession.read.parquet(LOCAL_PARENT_DIR + "reason.parquet");
      sparkSession.sqlContext.createDataFrame(reason.rdd, reasonSchema).registerTempTable("reason");
      //    reason.registerTempTable("reason");
    }

    // ============================================================> STORE
    if (tpcds_queries_store.contains(query)) {
      val store = sparkSession.read.parquet(LOCAL_PARENT_DIR + "store.parquet");
      //    store.registerTempTable("store");
      val storeSchema = StructType(
        StructField("s_store_sk", IntegerType, true) ::
          StructField("s_store_id", StringType, true) ::
          StructField("s_rec_start_date", StringType, true) ::
          StructField("s_rec_end_date", StringType, true) ::
          StructField("s_closed_date_sk", IntegerType, true) ::
          StructField("s_store_name", StringType, true) ::
          StructField("s_number_employees", IntegerType, true) ::
          StructField("s_floor_space", IntegerType, true) ::
          StructField("s_hours", StringType, true) ::
          StructField("s_manager", StringType, true) ::
          StructField("s_market_id", IntegerType, true) ::
          StructField("s_geography_class", StringType, true) ::
          StructField("s_market_desc", StringType, true) ::
          StructField("s_market_manager", StringType, true) ::
          StructField("s_division_id", IntegerType, true) ::
          StructField("s_division_name", StringType, true) ::
          StructField("s_company_id", IntegerType, true) ::
          StructField("s_company_name", StringType, true) ::
          StructField("s_street_number", StringType, true) ::
          StructField("s_street_name", StringType, true) ::
          StructField("s_street_type", StringType, true) ::
          StructField("s_suite_number", StringType, true) ::
          StructField("s_city", StringType, true) ::
          StructField("s_county", StringType, true) ::
          StructField("s_state", StringType, true) ::
          StructField("s_zip", StringType, true) ::
          StructField("s_country", StringType, true) ::
          StructField("s_gmt_offset", DecimalType(5, 2), true) ::
          StructField("s_tax_precentage", DecimalType(5, 2), true) :: Nil)
      sparkSession.sqlContext.createDataFrame(store.rdd, storeSchema).registerTempTable("store");
    }

    // ============================================================> TIME_DIM
    if (tpcds_queries_timedim.contains(query)) {
      val timedimSchema = StructType(
        StructField("t_time_sk", IntegerType, true) ::
          StructField("t_time_id", StringType, true) ::
          StructField("t_time", IntegerType, true) ::
          StructField("t_hour", IntegerType, true) ::
          StructField("t_minute", IntegerType, true) ::
          StructField("t_second", IntegerType, true) ::
          StructField("t_am_pm", StringType, true) ::
          StructField("t_shift", StringType, true) ::
          StructField("t_sub_shift", StringType, true) ::
          StructField("t_meal_time", StringType, true) :: Nil);
      val time_dim = sparkSession.read.parquet(LOCAL_PARENT_DIR + "time_dim.parquet");
      sparkSession.sqlContext.createDataFrame(time_dim.rdd, timedimSchema).registerTempTable("time_dim");
      //    time_dim.registerTempTable("time_dim");
    }

    // ============================================================> WEB SALES
    if (tpcds_queries_websales.contains(query)) {
      val websalesSchema = StructType(
        StructField("ws_sold_time_sk", IntegerType, true) :: //0
          StructField("ws_ship_date_sk", IntegerType, true) :: //1
          StructField("ws_item_sk", IntegerType, true) :: //2
          StructField("ws_bill_customer_sk", IntegerType, true) :: //3
          StructField("ws_bill_cdemo_sk", IntegerType, true) :: //4
          StructField("ws_bill_hdemo_sk", IntegerType, true) :: //5
          StructField("ws_bill_addr_sk", IntegerType, true) :: //6
          StructField("ws_ship_customer_sk", IntegerType, true) :: //7
          StructField("ws_ship_cdemo_sk", IntegerType, true) :: //8
          StructField("ws_ship_hdemo_sk", IntegerType, true) :: //9
          StructField("ws_ship_addr_sk", IntegerType, true) :: //10
          StructField("ws_web_page_sk", IntegerType, true) :: //11
          StructField("ws_web_site_sk", IntegerType, true) :: //12
          StructField("ws_ship_mode_sk", IntegerType, true) :: //13
          StructField("ws_warehouse_sk", IntegerType, true) :: //14
          StructField("ws_promo_sk", IntegerType, true) :: //15
          StructField("ws_order_number", IntegerType, true) :: //16
          StructField("ws_quantity", IntegerType, true) :: //17
          StructField("ws_wholesale_cost", DecimalType(7, 2), true) :: //18
          StructField("ws_list_price", DecimalType(7, 2), true) :: //19
          StructField("ws_sales_price", DecimalType(7, 2), true) :: //20
          StructField("ws_ext_discount_amt", DecimalType(7, 2), true) :: //21
          StructField("ws_ext_sales_price", DecimalType(7, 2), true) :: //22
          StructField("ws_ext_wholesale_cost", DecimalType(7, 2), true) :: //23
          StructField("ws_ext_list_price", DecimalType(7, 2), true) :: //24
          StructField("ws_ext_tax", DecimalType(7, 2), true) :: //25
          StructField("ws_coupon_amt", DecimalType(7, 2), true) :: //26
          StructField("ws_ext_ship_cost", DecimalType(7, 2), true) :: //27
          StructField("ws_net_paid", DecimalType(7, 2), true) :: //28
          StructField("ws_net_paid_inc_tax", DecimalType(7, 2), true) :: //29
          StructField("ws_net_paid_inc_ship", DecimalType(7, 2), true) :: //30
          StructField("ws_net_paid_inc_ship_tax", DecimalType(7, 2), true) :: //31
          StructField("ws_net_profit", DecimalType(7, 2), true) :: //32
          StructField("ws_sold_date_sk", IntegerType, true) :: Nil); //33
      if (!percentage.contains("") && (query.equals("q12") || query.equals("q23"))) {
        val web_sales = sparkSession.read.parquet(LOCAL_SAMPLE_DIR + "ws_strat_" + percentage + "_" + query + ".parquet");
        //    catalog_sales.registerTempTable("catalog_sales");
        sparkSession.sqlContext.createDataFrame(web_sales.rdd, websalesSchema).registerTempTable("web_sales");
      } else {
        val web_sales = sparkSession.read.parquet(LOCAL_PARENT_DIR + "web_sales.parquet");
        //    web_sales.registerTempTable("web_sales");
        sparkSession.sqlContext.createDataFrame(web_sales.rdd, websalesSchema).registerTempTable("web_sales");
      }
    }
    // ============================================================> WEB SITE
    if (tpcds_queries_website.contains(query)) {
      val websiteSchema = StructType(
        StructField("web_site_sk", IntegerType, true) ::
          StructField("web_site_id", StringType, true) ::
          StructField("web_rec_start_date", DateType, true) ::
          StructField("web_rec_end_date", DateType, true) ::
          StructField("web_name", StringType, true) ::
          StructField("web_open_date_sk", IntegerType, true) ::
          StructField("web_close_date_sk", IntegerType, true) ::
          StructField("web_class", StringType, true) ::
          StructField("web_manager", StringType, true) ::
          StructField("web_mkt_id", IntegerType, true) ::
          StructField("web_mkt_class", StringType, true) ::
          StructField("web_mkt_desc", StringType, true) ::
          StructField("web_market_manager", StringType, true) ::
          StructField("web_company_id", IntegerType, true) ::
          StructField("web_company_name", StringType, true) ::
          StructField("web_street_number", StringType, true) ::
          StructField("web_street_name", StringType, true) ::
          StructField("web_street_type", StringType, true) ::
          StructField("web_suite_number", StringType, true) ::
          StructField("web_city", StringType, true) ::
          StructField("web_county", StringType, true) ::
          StructField("web_state", StringType, true) ::
          StructField("web_zip", StringType, true) ::
          StructField("web_country", StringType, true) ::
          StructField("web_gmt_offset", StringType, true) ::
          StructField("web_tax_percentage", DecimalType(5, 2), true) :: Nil)
      val web_site = sparkSession.read.parquet(LOCAL_PARENT_DIR + "web_site.parquet");
      sparkSession.sqlContext.createDataFrame(web_site.rdd, websiteSchema).registerTempTable("web_site");
      //    web_site.registerTempTable("web_site");
    }
    // ============================================================> WEB RETURNS
    if (tpcds_queries_webreturns.contains(query)) {
      val webreturnsSchema = StructType(
        StructField("wr_returned_time_sk", LongType, true) ::
          StructField("wr_item_sk", LongType, true) ::
          StructField("wr_refunded_customer_sk", LongType, true) ::
          StructField("wr_refunded_cdemo_sk", LongType, true) ::
          StructField("wr_refunded_hdemo_sk", LongType, true) ::
          StructField("wr_refunded_addr_sk", LongType, true) ::
          StructField("wr_returning_customer_sk", LongType, true) ::
          StructField("wr_returning_cdemo_sk", LongType, true) ::
          StructField("wr_returning_hdemo_sk", LongType, true) ::
          StructField("wr_returning_addr_sk", LongType, true) ::
          StructField("wr_web_page_sk", LongType, true) ::
          StructField("wr_reason_sk", LongType, true) ::
          StructField("wr_order_number", LongType, true) ::
          StructField("wr_return_quantity", LongType, true) ::
          StructField("wr_return_amt", DecimalType(7, 2), true) ::
          StructField("wr_return_tax", DecimalType(7, 2), true) ::
          StructField("wr_return_amt_inc_tax", DecimalType(7, 2), true) ::
          StructField("wr_fee", DecimalType(7, 2), true) ::
          StructField("wr_return_ship_cost", DecimalType(7, 2), true) ::
          StructField("wr_refunded_cash", DecimalType(7, 2), true) ::
          StructField("wr_reversed_charge", DecimalType(7, 2), true) ::
          StructField("wr_account_credit", DecimalType(7, 2), true) ::
          StructField("wr_net_loss", DecimalType(7, 2), true) ::
          StructField("wr_returned_date_sk", IntegerType, true) :: Nil);
      val web_returns = sparkSession.read.parquet(LOCAL_PARENT_DIR + "web_returns.parquet");
      sparkSession.sqlContext.createDataFrame(web_returns.rdd, webreturnsSchema).registerTempTable("web_returns");
      //    web_returns.registerTempTable("web_returns");
    }
    // ============================================================> WAREHOUSE
    if (tpcds_queries_warehouse.contains(query)) {
      val warehouseSchema = StructType(
        StructField("w_warehouse_sk", IntegerType, true) ::
          StructField("w_warehouse_id", StringType, true) ::
          StructField("w_warehouse_name", StringType, true) ::
          StructField("w_warehouse_sq_ft", IntegerType, true) ::
          StructField("w_street_number", StringType, true) ::
          StructField("w_street_name", StringType, true) ::
          StructField("w_street_type", StringType, true) ::
          StructField("w_suite_number", StringType, true) ::
          StructField("w_city", StringType, true) ::
          StructField("w_county", StringType, true) ::
          StructField("w_state", StringType, true) ::
          StructField("w_zip", StringType, true) ::
          StructField("w_country", StringType, true) ::
          StructField("w_gmt_offset", DecimalType(5, 2), true) :: Nil)
      val warehouse = sparkSession.read.parquet(LOCAL_PARENT_DIR + "warehouse.parquet");
      sparkSession.sqlContext.createDataFrame(warehouse.rdd, warehouseSchema).registerTempTable("warehouse");
      //    warehouse.registerTempTable("warehouse");
    }
    // ============================================================> WEB PAGE
    if (tpcds_queries_web_page.contains(query)) {
      val webpageSchema = StructType(
        StructField("wp_web_page_sk", IntegerType, true) ::
          StructField("wp_web_page_id", StringType, true) ::
          StructField("wp_rec_start_date", DateType, true) ::
          StructField("wp_rec_end_date", DateType, true) ::
          StructField("wp_creation_date_sk", IntegerType, true) ::
          StructField("wp_access_date_sk", IntegerType, true) ::
          StructField("wp_autogen_flag", StringType, true) ::
          StructField("wp_customer_sk", IntegerType, true) ::
          StructField("wp_url", StringType, true) ::
          StructField("wp_type", StringType, true) ::
          StructField("wp_char_count", IntegerType, true) ::
          StructField("wp_link_count", IntegerType, true) ::
          StructField("wp_image_count", IntegerType, true) ::
          StructField("wp_max_ad_count", IntegerType, true) :: Nil)
      val web_page = sparkSession.read.parquet(LOCAL_PARENT_DIR + "web_page.parquet");
      sparkSession.sqlContext.createDataFrame(web_page.rdd, webpageSchema).registerTempTable("web_page");
      //    web_page.registerTempTable("web_page");
    }
    // ============================================================> CATALOG PAGE
    if (tpcds_queries_catalogpage.contains(query)) {
      val catalogpageSchema = StructType(
        StructField("cp_catalog_page_sk", IntegerType, true) ::
          StructField("cp_catalog_page_id", StringType, true) ::
          StructField("cp_start_date_sk", IntegerType, true) ::
          StructField("cp_end_date_sk", IntegerType, true) ::
          StructField("cp_department", StringType, true) ::
          StructField("cp_catalog_number", IntegerType, true) ::
          StructField("cp_catalog_page_number", IntegerType, true) ::
          StructField("cp_description", StringType, true) ::
          StructField("cp_type", StringType, true) :: Nil)
      val catalog_page = sparkSession.read.parquet(LOCAL_PARENT_DIR + "catalog_page.parquet");
      sparkSession.sqlContext.createDataFrame(catalog_page.rdd, catalogpageSchema).registerTempTable("catalog_page");
      //    catalog_page.registerTempTable("catalog_page");
    }
    // ============================================================> SHIP MODE
    if (tpcds_queries_shipmode.contains(query)) {
      val shipmodeSchema = StructType(
        StructField("sm_ship_mode_sk", IntegerType, true) ::
          StructField("sm_ship_mode_id", StringType, true) ::
          StructField("sm_type", StringType, true) ::
          StructField("sm_code", StringType, true) ::
          StructField("sm_carrier", StringType, true) ::
          StructField("sm_contract", StringType, true) :: Nil)
      val ship_mode = sparkSession.read.parquet(LOCAL_PARENT_DIR + "ship_mode.parquet");
      sparkSession.sqlContext.createDataFrame(ship_mode.rdd, shipmodeSchema).registerTempTable("ship_mode");
      //    ship_mode.registerTempTable("ship_mode");
    }
  }

  def load_skyServer_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    val SpecObjAll = sparkSession.read.parquet(DATA_DIR + "specobjall.parquet").drop(colName = "img");
    sparkSession.sqlContext.createDataFrame(SpecObjAll.rdd, SpecObjAll.schema).createOrReplaceTempView("specobjall");
    val PlateX = sparkSession.read.parquet(DATA_DIR + "plateX.parquet");
    sparkSession.sqlContext.createDataFrame(PlateX.rdd, PlateX.schema).createOrReplaceTempView("plateX");
    val SpecObj = sparkSession.read.parquet(DATA_DIR + "specobj.parquet").drop(colName = "img");
    sparkSession.sqlContext.createDataFrame(SpecObj.rdd, SpecObj.schema).createOrReplaceTempView("specobj");
    val PhotoPrimary = sparkSession.read.parquet(DATA_DIR + "photoprimary.parquet");
    sparkSession.sqlContext.createDataFrame(PhotoPrimary.rdd, PhotoPrimary.schema).createOrReplaceTempView("photoprimary");
    val specphoto = sparkSession.read.parquet(DATA_DIR + "specphoto.parquet");
    sparkSession.sqlContext.createDataFrame(specphoto.rdd, specphoto.schema).createOrReplaceTempView("specphoto");
    val photoobj = sparkSession.read.parquet(DATA_DIR + "photoobj.parquet");
    sparkSession.sqlContext.createDataFrame(photoobj.rdd, photoobj.schema).createOrReplaceTempView("photoobj");
    val PhotoObjAll = sparkSession.read.parquet(DATA_DIR + "photoobjall.parquet");
    sparkSession.sqlContext.createDataFrame(PhotoObjAll.rdd, PhotoObjAll.schema).createOrReplaceTempView("photoobjall");
    val galaxy = sparkSession.read.parquet(DATA_DIR + "galaxy.parquet");
    sparkSession.sqlContext.createDataFrame(galaxy.rdd, galaxy.schema).createOrReplaceTempView("galaxy");
    val GalaxyTag = sparkSession.read.parquet(DATA_DIR + "galaxytag.parquet");
    sparkSession.sqlContext.createDataFrame(GalaxyTag.rdd, GalaxyTag.schema).createOrReplaceTempView("galaxytag");
    val FIRST = sparkSession.read.parquet(DATA_DIR + "first.parquet");
    sparkSession.sqlContext.createDataFrame(FIRST.rdd, FIRST.schema).createOrReplaceTempView("first");
    val Field = sparkSession.read.parquet(DATA_DIR + "field.parquet");
    sparkSession.sqlContext.createDataFrame(Field.rdd, Field.schema).createOrReplaceTempView("field");
    val SpecPhotoAll = sparkSession.read.parquet(DATA_DIR + "specphotoall.parquet");
    sparkSession.sqlContext.createDataFrame(SpecPhotoAll.rdd, SpecPhotoAll.schema).createOrReplaceTempView("specphotoall");
    val sppParams = sparkSession.read.parquet(DATA_DIR + "sppparams.parquet");
    sparkSession.sqlContext.createDataFrame(sppParams.rdd, sppParams.schema).createOrReplaceTempView("sppparams");
    val wise_xmatch = sparkSession.read.parquet(DATA_DIR + "wise_xmatch.parquet");
    sparkSession.sqlContext.createDataFrame(wise_xmatch.rdd, wise_xmatch.schema).createOrReplaceTempView("wise_xmatch");
    val emissionLinesPort = sparkSession.read.parquet(DATA_DIR + "emissionlinesport.parquet");
    sparkSession.sqlContext.createDataFrame(emissionLinesPort.rdd, emissionLinesPort.schema).createOrReplaceTempView("emissionlinesport");
    val wise_allsky = sparkSession.read.parquet(DATA_DIR + "wise_allsky.parquet");
    sparkSession.sqlContext.createDataFrame(wise_allsky.rdd, wise_allsky.schema).createOrReplaceTempView("wise_allsky");
    val galSpecLine = sparkSession.read.parquet(DATA_DIR + "galspecline.parquet");
    sparkSession.sqlContext.createDataFrame(galSpecLine.rdd, galSpecLine.schema).createOrReplaceTempView("galspecline");
    val zooSPec = sparkSession.read.parquet(DATA_DIR + "zoospec.parquet");
    sparkSession.sqlContext.createDataFrame(zooSPec.rdd, zooSPec.schema).createOrReplaceTempView("zoospec");
    val Photoz = sparkSession.read.parquet(DATA_DIR + "photoz.parquet");
    sparkSession.sqlContext.createDataFrame(Photoz.rdd, Photoz.schema).createOrReplaceTempView("photoz");
    val zooNoSpec = sparkSession.read.parquet(DATA_DIR + "zoonospec.parquet");
    sparkSession.sqlContext.createDataFrame(zooNoSpec.rdd, zooNoSpec.schema).createOrReplaceTempView("zoonospec");
    val star = sparkSession.read.parquet(DATA_DIR + "star.parquet");
    sparkSession.sqlContext.createDataFrame(star.rdd, star.schema).createOrReplaceTempView("star");
    val propermotions = sparkSession.read.parquet(DATA_DIR + "propermotions.parquet");
    sparkSession.sqlContext.createDataFrame(propermotions.rdd, propermotions.schema).createOrReplaceTempView("propermotions");
    val stellarmassstarformingport = sparkSession.read.parquet(DATA_DIR + "stellarmassstarformingport.parquet");
    sparkSession.sqlContext.createDataFrame(stellarmassstarformingport.rdd, stellarmassstarformingport.schema).createOrReplaceTempView("stellarmassstarformingport");
    val sdssebossfirefly = sparkSession.read.parquet(DATA_DIR + "sdssebossfirefly.parquet");
    sparkSession.sqlContext.createDataFrame(sdssebossfirefly.rdd, sdssebossfirefly.schema).createOrReplaceTempView("sdssebossfirefly");
    val spplines = sparkSession.read.parquet(DATA_DIR + "spplines.parquet");
    sparkSession.sqlContext.createDataFrame(spplines.rdd, spplines.schema).createOrReplaceTempView("spplines");

    Paths.tableToCount.put("specobjall", SpecObjAll.count())
    Paths.tableToCount.put("platex", PlateX.count())
    Paths.tableToCount.put("specobj", SpecObj.count())
    Paths.tableToCount.put("photoprimary", PhotoPrimary.count())
    Paths.tableToCount.put("specphoto", specphoto.count())
    Paths.tableToCount.put("photoobj", photoobj.count())
    Paths.tableToCount.put("photoobjall", PhotoObjAll.count())
    Paths.tableToCount.put("galaxy", galaxy.count())
    Paths.tableToCount.put("galaxytag", GalaxyTag.count())
    Paths.tableToCount.put("first", FIRST.count())
    Paths.tableToCount.put("field", Field.count())
    Paths.tableToCount.put("specphotoall", SpecPhotoAll.count())
    Paths.tableToCount.put("sppparams", sppParams.count())
    Paths.tableToCount.put("wise_xmatch", wise_xmatch.count())
    Paths.tableToCount.put("emissionlinesport", emissionLinesPort.count())
    Paths.tableToCount.put("wise_allsky", wise_allsky.count())
    Paths.tableToCount.put("galspecline", galSpecLine.count())
    Paths.tableToCount.put("zoospec", zooSPec.count())
    Paths.tableToCount.put("photoz", Photoz.count())
    Paths.tableToCount.put("zoonospec", zooNoSpec.count())
    Paths.tableToCount.put("star", star.count())
    Paths.tableToCount.put("propermotions", propermotions.count())
    Paths.tableToCount.put("stellarmassstarformingport", stellarmassstarformingport.count())
    Paths.tableToCount.put("sdssebossfirefly", sdssebossfirefly.count())
    Paths.tableToCount.put("spplines", spplines.count())


    //val XXX = sparkSession.read.parquet(DATA_DIR + "XXX.parquet");
    //sparkSession.sqlContext.createDataFrame(XXX.rdd, XXX.schema).createOrReplaceTempView("XXX");
  }

  def load_atoka_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    val SCV = sparkSession.read.parquet(DATA_DIR + "SCV.parquet");
    sparkSession.sqlContext.createDataFrame(SCV.rdd, SCV.schema).createOrReplaceTempView("SCV");
    val PFV = sparkSession.read.parquet(DATA_DIR + "PFV.parquet");
    sparkSession.sqlContext.createDataFrame(PFV.rdd, PFV.schema).createOrReplaceTempView("PFV");
    val BRS = sparkSession.read.parquet(DATA_DIR + "BRS.parquet");
    sparkSession.sqlContext.createDataFrame(BRS.rdd, BRS.schema).createOrReplaceTempView("BRS");
    //val XXX = sparkSession.read.parquet(DATA_DIR + "XXX.parquet");
    //sparkSession.sqlContext.createDataFrame(XXX.rdd, XXX.schema).createOrReplaceTempView("XXX");
  }

  def load_test_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    val tab1 = sparkSession.read.parquet(DATA_DIR + "tab1.parquet");
    sparkSession.sqlContext.createDataFrame(tab1.rdd, tab1.schema).createOrReplaceTempView("tab1");
    val tab2 = sparkSession.read.parquet(DATA_DIR + "tab2.parquet");
    sparkSession.sqlContext.createDataFrame(tab2.rdd, tab2.schema).createOrReplaceTempView("tab2");
    //val XXX = sparkSession.read.parquet(DATA_DIR + "XXX.parquet");
    //sparkSession.sqlContext.createDataFrame(XXX.rdd, XXX.schema).createOrReplaceTempView("XXX");
  }

  def load_proteus_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    val tables = (new File(DATA_DIR)).listFiles.filter(x => x.isFile && x.getName.contains(".csv"))
    for (tableFile <- tables) {
      val tableName = tableFile.getName.substring(0, tableFile.getName.indexOf(".") - 1)
      val tab = sparkSession.read.parquet(DATA_DIR + tableFile.getName);
      sparkSession.sqlContext.createDataFrame(tab.rdd, tab.schema).createOrReplaceTempView(tableName);
    }
  }

}