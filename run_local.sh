export INPUT_BUCKET=<BUCKET_NAME>
export PROJECT=<PROJECT_ID>
export SRC_PATH=src/main.py

python $SRC_PATH --input_bucket $INPUT_BUCKET \
      --project $PROJECT --environment local\
      --bq_input_table fh-bigquery.hackernews.storiesV2\
      --bq_output_table news.hacker_news_stories