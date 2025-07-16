#!/bin/bash

# Get username
WHOAMI="$(whoami)"

# Create virtual environment
python3 -m venv ~/airflow/env

# Set Airflow version
AIRFLOW_VERSION=3.0.2

# Get Python version (e.g., 3.11)
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

# Define constraints URL 
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Airflow 
~/airflow/env/bin/pip install "apache-airflow[postgres]==$AIRFLOW_VERSION" --constraint "$CONSTRAINT_URL"

# Setup AIRFLOW_HOME
export AIRFLOW="/home/$WHOAMI/airflow"

# Setup PYTHONPATH to store custom libraries to be used in DAGs
mkdir ~/airflow/libs
export PYTHONPATH="$PYTHONPATH:/home/$WHOAMI/airflow/libs" 

# Create directories for Airflow
mkdir ~/airflow/dags
mkdir ~/airflow/plugins

# Keep these folders available
touch ~/airflow/dags/.gitkeep
touch ~/airflow/plugins/.gitkeep