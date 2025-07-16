# Airflow Project

This project sets up and manages an Apache Airflow environment for orchestrating workflows and data pipelines.

## Project Structure

- `dags/` - Contains Airflow DAG definitions.
- `libs/` - Contains the custom packages / libraries built for DAGs.
- `plugins/` - Custom Airflow plugins.
- `scripts/` - Shell scripts for setting up environment.
- `airflow.cfg` - Airflow configuration file.
- `README.md` - Project documentation (this file).

## Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone https://github.com/imjbmkz/PetShopScraperPipeline.git
   cd airflow
   ```
2. **Check if Python is installed. Python 3.10>**
   ```bash
   python3 --version
   ```
3. **Create airflow directory.**
   ```bash
   mkdir airflow
   ```
4. **Install Airflow and dependencies:**
   Use the provided scripts in `scripts/` for automated setup:
   ```bash
   cd scripts
   ./00_setup_postgres_airflow.sh
   ```
5. **Configure Airflow:**
   - Edit `airflow.cfg` as needed for your environment.
   - Place your DAGs in the `dags/` directory.

## Usage

- Add your DAGs to the `dags/` directory.
- Add your custom packages and libraries in `libs/` folder.
- Monitor and manage workflows via the Airflow web UI (default: http://localhost:8080).
- Logs are stored in the `logs/` directory.

## Customization

- Add custom plugins to the `plugins/` directory.
- Modify or add setup scripts in `scripts/` as needed.

## Troubleshooting

- Check logs in the `logs/` directory for errors.
- Ensure all services are running (scheduler, webserver, etc.).
- Review the output of setup scripts for any issues.

## License

This project is licensed under the [Apache License 2.0](./LICENSE).

# TODO
Document the best way of managing virtual environments for multiple projects.