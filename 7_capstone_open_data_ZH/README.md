[ ] Move historical data to S3, 2 folders, 1 for x verkehrszählungs csv, 2 for 1 wetter csv
[ ] OPTION: Daily aggregations on air quality and rain duration

[ ] Install airflow
[ ] Set-up AIRFLOW DAG(s) for daily updates
    - verkehrszählungen from the same csv load only difference or rows with datestamp of yesterday
    - wetter from API, query for yesterday's data
    - append where appropriate

[ ] Organize normalized data (core DB)
    1 dimension for standorte, with LONG, LAT or eventually geoJSON
    1 dimension for time / date with is it weekend etc.
    2 fact tables for wetter and verkehrszählungen

[ ] Export a dataqube or flatfile (parquet?) for predictions with some metrics to S3
    ...

## Resources

### DateDim

https://wiki.postgresql.org/wiki/Date_and_Time_dimensions
https://nicholasduffy.com/posts/postgresql-date-dimension/
