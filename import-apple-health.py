import click
import pathlib
import pyodbc
from influxdb import InfluxDBClient
import logging
import zipfile
import re
import datetime
from lxml import etree as ET


@click.command()
@click.option('--log-level', type=click.Choice(['debug', 'info'], case_sensitive=False), default="info", show_default=True)
@click.option('--influxdb-host',default="localhost",help="Host running the influx db",show_default=True)
@click.option('--influxdb-dbname',default="healthdata",help="InfluxDB database name",show_default=True)
@click.argument('apple_healh_export_zip',type=click.Path(exists=True,dir_okay=False))
def cli (log_level,influxdb_host,influxdb_dbname,apple_healh_export_zip):
    """
        Imports an export of apple health into an influxdb
    """
    if str(log_level) == "debug":
        logging.basicConfig(level=logging.DEBUG,
                            format='[%(levelname)s] %(name)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO,
                            format='[%(levelname)s] %(name)s - %(message)s')
    influxdb = InfluxDBClient(host=influxdb_host, port=8086)
    influxdb.create_database(influxdb_dbname)
    health_data_zipfile = zipfile.ZipFile(apple_healh_export_zip)
    record_types = set()
    num_records = 0
    result = list(influxdb.query ("Select last(latest_imported_timestamp) from Import",database=influxdb_dbname).get_points()) # get last timepoint that was inserted
    if len (result)>0:
        previous_import_timestamp = result[0]['last']
        logging.info (f"Importing new records since previous import timestamp {previous_import_timestamp}")
    else:
        previous_import_timestamp = "1970-01-01"
        logging.info ("No previous import found. Importing all records")
    latest_imported_timestamp = "1970-01-01"
    with health_data_zipfile.open("apple_health_export/Export.xml") as export_xml_file:
        export_xml = ET.parse(export_xml_file)
        for record in export_xml.getroot().iter("Record"):
            m = re.match ("HKQuantityTypeIdentifier(.*)",record.attrib['type'])
            if m is not None:
                record_type = m.group(1)
                if record.attrib['startDate'] < previous_import_timestamp:
                    continue
                if record.attrib['startDate'] > latest_imported_timestamp:
                    latest_imported_timestamp = record.attrib['startDate']
                if not record_type in record_types:
                    logging.info (f"Found record type {record_type}")
                    record_types.add(record_type)
                num_records = num_records + 1
                json_data = [
                    {
                        "measurement": record_type,
                        "time": record.attrib['startDate'],
                        "fields": {
                            "value":  float(record.attrib['value'])
                        },
                        "tags": {
                            "sourceName": record.attrib['sourceName']
                        }
                    }
                ]
                influxdb.write_points(json_data,database=influxdb_dbname)
        for record in export_xml.getroot().iter("Workout"):
            m = re.match ("HKWorkoutActivityType(.*)",record.attrib['workoutActivityType'])
            if m is not None:
                record_type = m.group(1)
                if record.attrib['startDate'] < previous_import_timestamp:
                    continue
                if record.attrib['startDate'] > latest_imported_timestamp:
                    latest_imported_timestamp = record.attrib['startDate']
                if not record_type in record_types:
                    logging.info (f"Found record type {record_type}")
                    record_types.add(record_type)
                num_records = num_records + 1
                json_data = [
                    {
                        "measurement": "Workout",
                        "time": record.attrib['startDate'],
                        "fields": {
                            "duration": float(record.attrib['duration']),
                            "totalDistance": float(record.attrib['totalDistance']),
                            "totalEnergyBurned": float(record.attrib['totalEnergyBurned'])
                        },
                        "tags": {
                            "sourceName": record.attrib['sourceName'],
                            "workoutActivityType": record_type
                        }
                    }
                ]
                influxdb.write_points(json_data,database=influxdb_dbname)
        for record in export_xml.getroot().iter("ActivitySummary"):
            record_type = "ActivitySummary"
            if record.attrib['dateComponents'] < previous_import_timestamp:
                continue
            if record.attrib['dateComponents'] > latest_imported_timestamp:
                    latest_imported_timestamp = record.attrib['dateComponents']
            if not record_type in record_types:
                logging.info (f"Found record type {record_type}")
                record_types.add(record_type)
            num_records = num_records + 1
            json_data = [
                {
                    "measurement": "ActivitySummary",
                    "time": record.attrib['dateComponents'],
                    "fields": {
                        "activeEnergyBurned": float(record.attrib['activeEnergyBurned']),
                        "activeEnergyBurnedGoal": float(record.attrib['activeEnergyBurnedGoal']),
                        "appleMoveTime": float(record.attrib['appleMoveTime']),
                        "appleMoveTimeGoal": float(record.attrib['appleMoveTimeGoal']),
                        "appleExerciseTime": float(record.attrib['appleExerciseTime']),
                        "appleExerciseTimeGoal": float(record.attrib['appleExerciseTimeGoal']),
                        "appleStandHours": float(record.attrib['appleStandHours']),
                        "appleStandHoursGoal": float(record.attrib['appleStandHoursGoal'])
                    }
                }
            ]
            influxdb.write_points(json_data,database=influxdb_dbname)
    json_data = [
        {
            "measurement": "Import",
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fields": {
                "file": apple_healh_export_zip,
                "latest_imported_timestamp": latest_imported_timestamp
            }
        }
    ]
    logging.info (f"Latest imported timestamp: {latest_imported_timestamp}")
    influxdb.write_points(json_data,database=influxdb_dbname)
    logging.info (f"Imported {num_records} records with {len(record_types)} different record types")


if __name__ == "__main__":
    cli()
