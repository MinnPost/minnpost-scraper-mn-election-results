from datetime import datetime
from datetime import timedelta
from dateutil import parser
import pytz
from datetimerange import DateTimeRange
from flask import current_app

class ElectionResultWindow(object):

    def __init__(self, Election = None):
        self.election = Election

    def check_election_result_window(self):
        election_result_window = {}
        is_election_result_window = False
        now = datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))
        interval = current_app.config["DEFAULT_SCRAPE_FREQUENCY"]
        debug_message = ""
        datetime_overridden = current_app.config["ELECTION_RESULT_DATETIME_OVERRIDDEN"]
        # set up the task interval based on the configuration values and/or the current datetime.
        if datetime_overridden == "true":
            is_election_result_window = True
            interval      = current_app.config["ELECTION_DAY_RESULT_SCRAPE_FREQUENCY"]
            debug_message = f"The current schedule is overridden and set to {interval} by a true value on the datetime override value."
        elif datetime_overridden == "false":
            is_election_result_window = False
            interval = current_app.config["DEFAULT_SCRAPE_FREQUENCY"]
            debug_message = f"The current schedule is overridden and set to {interval} by a false value on the datetime override value."
        else:
            if current_app.config["ELECTION_DAY_RESULT_HOURS_START"] != "" and current_app.config["ELECTION_DAY_RESULT_HOURS_END"] != "":
                is_election_result_window = True
                time_range = DateTimeRange(current_app.config["ELECTION_DAY_RESULT_HOURS_START"], current_app.config["ELECTION_DAY_RESULT_HOURS_END"])
                debug_message = f"This task is controlled by the start and end configuration values. "
            elif self.election.date:
                is_election_result_window = True
                start_after_hours = current_app.config["ELECTION_DAY_RESULT_DEFAULT_START_TIME"]
                end_after_hours   = current_app.config["ELECTION_DAY_RESULT_DEFAULT_DURATION_HOURS"]
                start_date_string = f"{self.election.date}T{start_after_hours}:00:00-0600"
                start_datetime    = parser.parse(start_date_string)
                end_datetime      = start_datetime + timedelta(hours=end_after_hours)

                time_range = DateTimeRange(start_datetime, end_datetime)
                debug_message = f"This task is controlled by the default timeframe with the {self.election.date} election. It will start {start_after_hours} hours into {self.election.date} and end after {end_after_hours} hours. "
            
            if time_range:
                now_formatted = now.isoformat()
                if now_formatted in time_range:
                    is_election_result_window = True
                    interval = current_app.config["ELECTION_DAY_RESULT_SCRAPE_FREQUENCY"]
                    debug_message += f"This task is being run during election result hours."
                else:
                    is_election_result_window = False
                    debug_message += f"This task is not being run during election result hours."
            else:
                is_election_result_window = False
                debug_message += f"This task has no time range, so it is not being run during election result hours."

        election_result_window["is_election_result_window"] = is_election_result_window
        election_result_window["interval"] = interval
        election_result_window["debug_message"] = debug_message
        return election_result_window
