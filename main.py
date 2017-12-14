import Config
import logging
import OperationsScheduler
import time
from datetime import datetime
from dao.RuleDataAccessor import RuleDataAccessor
from influxdb import InfluxDBClient

logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

pgConnection = Config.Configuration().getDatabaseConnection()
ruleDao = RuleDataAccessor(pgConnection)

def processRules(args):
    influxClient = args[0]
    print("process rules")
    rules = ruleDao.getAll()
    # TODO: for each rule that has status "training", check if it's been one week.
    for rule in rules:
        if rule["status"] == "training":
            ruleAge = datetime.now() - rule["created_at"]
            if ruleAge.seconds >= 7200:
            # TODO: if it's been one week, get the average moisture for that sensor for the past week
                print("rule is 1 week old")
                average = influxClient.query("select mean(value) from moisture where device_id = '" +
                    rule["sensor_id"] + "' and time > now() - 2h")
                print average
        # TODO: store this average moisture value as the rule's 'threshold' property

def getInfluxClient():
    print ("Connecting to influxdb host " + Config.Configuration().influxdbHost)
    influxClient = InfluxDBClient(
        Config.Configuration().influxdbHost,
        8086,
        Config.Configuration().influxdbUsername,
        Config.Configuration().influxdbPassword,
        'dionysus_readings')
    print "influxclient:"
    print influxClient
    return influxClient


def main():
    influxClient = getInfluxClient()
    # TODO: every 15 minutes, get all rules from postgres
    OperationsScheduler.asyncRunAtInterval(processRules, 1, (influxClient,), repeat=True)
    while(True):
        time.sleep(1)

main()
