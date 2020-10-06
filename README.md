# Apple Health Import into InfluxDB and Grafana

This project provides influxDB and grafana container and a script to import an Apple Health export.

## Usage

### Start Container

With ```docker-compose up -d``` the influxDB and the grafana container are beeing created and started in the background. Grafana is automatically provisioned with the [influxdb datasource](grafana/provisioning/datasources/influxdb.yml) and a set of [preconfigured dashboards](grafana/dashboards)

### Install prerequisits
Run ```pip3 install -r requirements.txt```to instal required python dependencies

### Export Apple Health
On your iPhone open health app go to your profile and select "Export Health Data".

### Import Health Data into InfluxDB
Run ```python3 import-apple-health.py Export.zip```where Export.zip is the exported file from your iPhone. On a first run with an Health export covering multiple years the import could last for quite a long time (> 1h).
On subsequent imports the script will only data that is newer than the previous import

### View Apple Health Data in Grafana
Open your browser and navigate to http://localhost:3000.
The default credentials are admin/admin