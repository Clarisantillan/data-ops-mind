from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.functions import col, from_unixtime

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL Process with PySpark") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.some.config.option", "config-value") \
        .config("spark.sql.files.maxPartitionBytes", "200m") \
        .getOrCreate()

def get_schema_meta():
    schema = StructType([
        StructField("MISC", StructType([
            StructField("Accessibility", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Activities", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Amenities", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Atmosphere", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Crowd", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Dining options", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("From the business", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Getting here", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Health & safety", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Highlights", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Lodging options", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Offerings", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Payments", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Planning", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Popular for", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Recycling", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("Service options", ArrayType(StringType(), containsNull=True), nullable=True),
        ]), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("avg_rating", DoubleType(), nullable=True),
        StructField("category", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("gmap_id", StringType(), nullable=True),
        StructField("hours", ArrayType(ArrayType(StringType(), containsNull=True), containsNull=True), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("num_of_reviews", LongType(), nullable=True),
        StructField("price", StringType(), nullable=True),
        StructField("relative_results", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
    ])
    return schema


us_states = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY"
}

def get_state_acronym(state_name):   
    state_name = state_name.title().replace("_", " ")
    return us_states.get(state_name, "State not found")

#Convertir el tiempo de gmap
def convert_timestamp_to_date(timestamp):
    return from_unixtime(timestamp / 1000, "yyyy-MM-dd")