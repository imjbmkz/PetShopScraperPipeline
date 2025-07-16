# Copy the pre-defined systemd service files to the appropriate directory
sudo cp ~/airflow/scripts/services/airflow-api-server.service /etc/systemd/system/airflow-api-server.service
sudo cp ~/airflow/scripts/services/airflow-dag-processor.service /etc/systemd/system/airflow-dag-processor.service
sudo cp ~/airflow/scripts/services/airflow-scheduler.service /etc/systemd/system/airflow-scheduler.service
sudo cp ~/airflow/scripts/services/airflow-triggerer.service /etc/systemd/system/airflow-triggerer.service

# Reload systemd to recognize the new service files
sudo systemctl daemon-reexec
sudo systemctl daemon-reload

# Enable the Airflow services
sudo systemctl enable airflow-api-server
sudo systemctl enable airflow-dag-processor
sudo systemctl enable airflow-scheduler
sudo systemctl enable airflow-triggerer

# Start the Airflow services
sudo systemctl start airflow-api-server
sudo systemctl start airflow-dag-processor
sudo systemctl start airflow-scheduler
sudo systemctl start airflow-triggerer