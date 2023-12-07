from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when, lower, monotonically_increasing_id, udf
from pyspark.sql.types import IntegerType, FloatType, DoubleType

import os
import yaml
config = yaml.safe_load(open("/opt/airflow/config/config.yaml"))


def filenames_func(directory_path):
    
    files = [i for i in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, i))]
    files_filtered = [os.path.join(directory_path,f) for f in files if not f.startswith('.')]
    
    return files_filtered

def create_df(spark,file_paths):
    new_df = spark.read.parquet(file_paths[0])
    for i in file_paths[1:]:
        new_df = new_df.\
                    union(spark.read.\
                    parquet(i))
    
    return new_df

DIM_tables_dict = {}

def transformation():

    
    spark = SparkSession.builder.\
                master('local').\
                appName('project').\
                config("spark.jars", "/opt/airflow/config/postgresql-42.7.0.jar").\
                getOrCreate()
    
    yellow_df = create_df(spark,filenames_func('/opt/airflow/data/yellowData/'))
    print('number of records in yellow taxi data : ', yellow_df.count())
    yellow_df = yellow_df.limit(10000)
    # lower case all column names
    for col in yellow_df.columns:
        yellow_df = yellow_df.withColumnRenamed(col, col.lower())

    # VendorID
    # cast to int
    from pyspark.sql.functions import col
    yellow_df = yellow_df.withColumn('vendorid', col('vendorid').cast(IntegerType()))
    # can only have value 1 or 2 acc to data dictionary, drop other rows
    yellow_df = yellow_df.filter(col('vendorid').isin(1,2))

    #Passenger Count
    # cast to int
    yellow_df = yellow_df.withColumn('passenger_count', col('passenger_count').\
                                    cast(IntegerType()))
    #remove None records
    yellow_df = yellow_df.dropna(how = 'all', subset = ['passenger_count'])
    #remove records with 0 as passenger count
    yellow_df = yellow_df.filter(col('passenger_count') > 0)

    # cast to float
    yellow_df = yellow_df.withColumn('trip_distance', col('trip_distance').cast(FloatType()))
    #drop rows with trip distance = 0
    yellow_df = yellow_df.filter((col('trip_distance') != 0) & (col('trip_distance') > 0))

    # cast location id to int
    yellow_df = yellow_df.\
                    withColumn('pulocationid', col('pulocationid').cast(IntegerType())).\
                    withColumn('dolocationid', col('dolocationid').cast(IntegerType()))
    
    ## cast ratecode id to int
    yellow_df = yellow_df.\
                    withColumn('ratecodeid', col('ratecodeid').cast(IntegerType()))
    
    ## drop rows with invalid ratecode id
    yellow_df = yellow_df.\
                    filter(col('ratecodeid').isin(1,2,3,4,5,6))
    
    # convert values to lowercase
    yellow_df = yellow_df.withColumn('store_and_fwd_flag', lower(col('store_and_fwd_flag')))

    ## adding a column saying whether the fare amount is +ve or -ve
    yellow_df = yellow_df.\
                    withColumn('errordata',when(col('fare_amount') < 0, 'y').\
                    otherwise('n'))

    ## casting the column to double

    yellow_df = yellow_df.\
                    withColumn('fare_amount', col('fare_amount').cast(DoubleType()))
    print("at non negative columns")
    non_negative_condition =(
                            (col('fare_amount') >= 0)& 
                            (col('extra') >= 0) &
                            (col('mta_tax') >= 0) &
                            (col('tip_amount') >= 0) &
                            (col('tolls_amount') >= 0) &
                            (col('improvement_surcharge') >= 0) &
                            (col('total_amount') >= 0) &
                            (col('congestion_surcharge') >= 0) &
                            (col('airport_fee') >= 0) 
    )

    yellow_df = yellow_df.\
            withColumn('errordata',when(non_negative_condition, 'n').\
            otherwise('y'))
    
    ## create vendor DIM
    @udf
    def vendor_name_udf(vendorid):
        vendor_dict = {
                        1 : 'Creative Mobile Technologies, LLC',
                    2 : 'VeriFone Inc.'
        }
        if vendorid is not None:
            return vendor_dict[vendorid]

    vendor_DIM = yellow_df.\
        select('vendorid').\
        distinct().\
        withColumn('vendor_key', monotonically_increasing_id()).\
        withColumn('vendor_name', vendor_name_udf(col('vendorid')))
    DIM_tables_dict['vendor_DIM'] = vendor_DIM
    
    ## create the times DIM
    pu_times = yellow_df.\
        select(col('tpep_pickup_datetime').alias('timestamp')).\
        dropDuplicates()

    do_times = yellow_df.\
        select(col('tpep_dropoff_datetime').alias('timestamp')).\
        dropDuplicates()
    
    datetime_DIM = pu_times.\
        union(do_times).\
        dropDuplicates().\
        withColumn('datetime_key', monotonically_increasing_id())
    DIM_tables_dict['datetime_DIM'] = datetime_DIM
    
    ## create locations DIM
    pu_locations = yellow_df.\
        select(col('pulocationid').alias('locationid')).\
        dropDuplicates()
    
    do_locations = yellow_df.\
        select(col('dolocationid').alias('locationid')).\
        dropDuplicates()
    
    taxizone_DIM = pu_locations.\
        union(do_locations).\
        dropDuplicates().\
        withColumn('zone_key', monotonically_increasing_id())
    DIM_tables_dict['taxizone_DIM'] = taxizone_DIM
    
    ## create payment type DIM
    @udf
    def paymenttype_name_udf(payment_type):
        payment_type_dict = {
                        1 : 'Credit card',
                        2 : 'Cash',
                        3 : 'No charge',
                        4 : 'Dispute',
                        5 : 'Unknown',
                        6 : 'Voided trip'
        }
        if payment_type is not None:
            return payment_type_dict[payment_type]

    paymenttype_DIM = yellow_df.\
        select('payment_type').\
        distinct().\
        withColumn('paymenttype_key', monotonically_increasing_id()).\
        withColumn('description', paymenttype_name_udf(col('payment_type')))
    DIM_tables_dict['paymenttype_DIM'] = paymenttype_DIM
    
    ## create rate code DIM
    @udf
    def ratecode_name_udf(ratecode):
        ratecode_dict = {
                        1 : 'Standard rate',
                        2 : 'JFK',
                        3 : 'Newark',
                        4 : 'Nassau or Westchester',
                        5 : 'Negotiated fare',
                        6 : 'Group ride'
        }
        if ratecode is not None:
            return ratecode_dict[ratecode]

    ratecode_DIM = yellow_df.\
        select('ratecodeid').\
        distinct().\
        withColumn('ratecode_key', monotonically_increasing_id()).\
        withColumn('description', ratecode_name_udf(col('ratecodeid')))
    DIM_tables_dict['ratecode_DIM'] = ratecode_DIM

    ## swapped pickup datetime with putime_key
    fact_table = yellow_df.join(datetime_DIM, yellow_df.tpep_pickup_datetime == datetime_DIM.timestamp, 'inner').\
                    drop('tpep_pickup_datetime','timestamp').\
                    withColumnRenamed('datetime_key', 'putime_key')
    
    ## swapped dropoff datetime with dotime_key
    fact_table = fact_table.join(datetime_DIM, fact_table.tpep_dropoff_datetime == datetime_DIM.timestamp, 'inner').\
                    drop('tpep_dropoff_datetime','timestamp').\
                    withColumnRenamed('datetime_key', 'dotime_key')
    
    ## swapped vendor id with vendor_key
    fact_table = fact_table.join(vendor_DIM, 'vendorid', 'inner').\
                    drop('vendorid','vendor_name')
    
    ## swapped ratecodeid with ratecode_key
    fact_table = fact_table.join(ratecode_DIM, 'ratecodeid', 'inner').\
                    drop('ratecodeid','description')
    
    ## pickup location id
    fact_table = fact_table.join(taxizone_DIM, fact_table.pulocationid == taxizone_DIM.locationid, 'inner').\
                    drop('pulocationid','locationid').\
                    withColumnRenamed('zone_key', 'puzone_key')
    
    ## dropoff location id
    fact_table = fact_table.join(taxizone_DIM, fact_table.dolocationid == taxizone_DIM.locationid, 'inner').\
                    drop('dolocationid','locationid').\
                    withColumnRenamed('zone_key', 'dozone_key')
    
    ## payment type
    fact_table = fact_table.join(paymenttype_DIM, 'payment_type', 'inner').\
                    drop('payment_type','description')
    
    cols_in_order = ['vendor_key',
                 'putime_key',
                 'dotime_key',
                 'passenger_count',
                 'trip_distance',
                 'ratecode_key',
                 'store_and_fwd_flag',
                 'puzone_key',
                 'dozone_key',
                 'paymenttype_key',
                 'fare_amount',
                 'extra',
                 'mta_tax',
                 'tip_amount',
                 'tolls_amount',
                 'improvement_surcharge',
                 'total_amount',
                 'congestion_surcharge',
                 'airport_fee',
                 'errordata']
    
    ## ordering columns in the fact table
    # fact_table = fact_table[*cols_in_order]

    ## saving tables to postgresql
    connection_string = f"jdbc:postgresql://{config['connection_params']['host']}:{config['connection_params']['port']}/{config['connection_params']['database']}"
    
    for table_name in DIM_tables_dict.keys():
        print(f"saving table {table_name}")
        
        DIM_tables_dict[table_name].write \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", table_name) \
                .option("user", config['connection_params']['user']) \
                .option("password", config['connection_params']['password']) \
                .option("driver", "org.postgresql.Driver")\
                .mode("overwrite") \
                .save()
    
    ## saving fact table
    fact_table.write\
            .format("jdbc") \
            .option("url", connection_string) \
            .option("dbtable", 'fact_table') \
            .option("user", config['connection_params']['user']) \
            .option("password", config['connection_params']['password']) \
            .option("driver", "org.postgresql.Driver")\
            .mode("overwrite") \
            .save()