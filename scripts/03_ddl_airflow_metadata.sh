#!/bin/bash

# Variables
DB_NAME="airflow"
DB_USER="airflow"
DB_PASS="airflow"

# Run SQL as the postgres user
sudo -u postgres psql <<EOF
-- Create airflow database
CREATE DATABASE $DB_NAME;

-- Create user and set password
CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';

-- Optional but recommended configurations
ALTER ROLE $DB_USER SET client_encoding TO 'utf8';
ALTER ROLE $DB_USER SET default_transaction_isolation TO 'read committed';
ALTER ROLE $DB_USER SET timezone TO 'UTC';

-- Grant all privileges on the database
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
EOF

# Grant permission on public schema
sudo -u postgres psql -d $DB_NAME <<EOF
GRANT ALL ON SCHEMA public TO $DB_USER;
EOF

# Setup connection string to Postgres database
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$DB_USER:$DB_PASS@localhost/$DB_NAME"