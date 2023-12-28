# online-retail-data-engineering
Creating an Online Retail Data Engineering Project using Google BigQuery, Airflow and Data Build Tool (DBT)

# Dataset Kaggle Link
Link-: https://www.kaggle.com/datasets/tunguz/online-retail/

# Install Astro CLI
brew install astro

# Initiate the Astro CLI for Airflow
astro dev init

# Start the Airflow Server 
astro dev start

# Using Astro SDK for loading the data to BQ 

# After the first load is done, use SODA to do Data Quality Checks

# Run command to check if the airflow server shows adress already in use 
sudo lsof -i :5432