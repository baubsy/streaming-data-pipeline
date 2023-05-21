```
CREATE EXTERNAL TABLE IF NOT EXISTS bswyers.enriched_reviews_external(
  user_id STRING,
  mail STRING,
  marketplace STRING,
  review_id STRING,
  product_id STRING,
  product_parent STRING,
  product_title STRING,
  product_category STRING,
  username STRING,
  hepful_votes STRING,
  total_votes STRING,
  vine STRING,
  verified_purchase STRING,
  review_headline STRING,
  review_body STRING,
  review_date STRING,
  birthdate STRING,
  name STRING,
  sex STRING
)
PARTITIONED by (star_rating STRING)
ROW FORMAT
  DELIMITED FIELDS TERMINATED BY '\t'
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/bswyers/reviews_csv/'


```