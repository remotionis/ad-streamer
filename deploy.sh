echo "Infra auto setup started"

echo "python & locust installation"
sudo apt update
sudo apt install -y python3 python3-venv netcat-openbsd

python3 -m venv locust_env

. locust_env/bin/activate

pip install locust pyyaml confluent_kafka

deactivate

echo "docker installation - debian"
sudo apt install -y gnome-terminal
sudo apt-get install -y ./docker-desktop-amd64.deb

:<<'END'

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

echo "Airflow init started"
docker compose up airflow-init

END

echo "Infra setup & auto-start completed"
