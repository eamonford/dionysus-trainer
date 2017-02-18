import Config
import logging
import OperationsScheduler
import time
from dao.RuleDataAccessor import RuleDataAccessor

logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

pgConnection = Config.Configuration().getDatabaseConnection()
ruleDao = RuleDataAccessor(pgConnection)

def processRules(args):
    print("process rules")
    rules = ruleDao.getAll()

def main():
    # TODO: every 15 minutes, get all rules from postgres
    OperationsScheduler.asyncRunAtInterval(processRules, 1, None, repeat=True)
    while(True):
        time.sleep(1)
    # TODO: for each rule that has status "training", check if it's been one week.
    # TODO: if it's been one week, get the average moisture for that sensor for the past week
    # TODO: store this average moisture value as the rule's 'threshold' property

main()
