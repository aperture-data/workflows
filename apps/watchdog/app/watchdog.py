import os
import argparse
import time
import json

from itertools import product

from aperturedb import CommonLibrary

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def check_host(db):

    is_ok = True
    last_response = ""

    text = ""
    try:
        db.query([{"GetStatus": {}}])

        if not db.last_query_ok():
            is_ok = False

        last_response = db.get_last_response_str()
        print(f" {db.last_query_time * 1000} ms", flush=True)
    except Exception as e:
        is_ok = False
        # print(e)

    return is_ok, last_response


def main(params):

    if params.post_to_slack:
        client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))

    print(f"Creating db object...", flush=True)
    db = CommonLibrary.create_connector()

    healthcheck_message_id = ''
    checks_counter = 0
    summaries = {}
    checks_in_1_day = 24 * 3600 / params.frequency_seconds
    checks_in_2_minutes = 2 * 60 / params.frequency_seconds

    if params.post_to_slack:

        message_watchdog_down = f":fire: :dog: Watchdog {params.watchdog_name} is down."
        message_watchdog_up = f":white_check_mark: :dog: Watchdog {params.watchdog_name} is up."

        # Let channel know watchdog is up
        try:
            result = client.chat_postMessage(
                channel="#" + params.channel,
                text=message_watchdog_up)
        except:
            print(f"Failed to post slack message (up).")

        # Remove previously scheduled messages
        try:
            result = client.chat_scheduledMessages_list(
                channel=result.data["channel"])
            for message in result.data["scheduled_messages"]:
                if message["text"] == message_watchdog_down:
                    if client.chat_deleteScheduledMessage(
                            channel="#" + params.channel,
                            scheduled_message_id=message["id"]):
                        print(f"Removed old message: " + message["id"])
                    else:
                        print(f"Failed to remove old message: " +
                              message["id"])
        except SlackApiError as e:
            print(f"Removing old messages threw SlackApiError: {e.response['error']}")
        except Exception as e:
            print(f"Removing old messages threw an unexpected exception: {str(e)}")
    bad = False
    while True:

        hostname = os.environ.get('DB_HOST', 'localhost')
        print(f"Checking {hostname}...", flush=True)

        is_ok, last_response = check_host(db)

        text = ""

        if is_ok:
            if bad:
                text += f":white_check_mark: `{hostname}` recovered and it is up again."
                bad = False
        elif bad:
            print(f"Already notified down: {hostname}")
        else:
            bad = True

            five_minutes_ago = int(time.time() * 1000) - 300000
            five_minutes_later = five_minutes_ago + 600000
            url = f"<https://{hostname}/grafana/d/mPHHiqbnk/aperturedb-connectivity-status?from={five_minutes_ago}&to={five_minutes_later}&var-job_filter=job%7C%3D%7Caperturedb&var-pod_ip=All&var-node_filter=pod_ip%7C%3D~%7C$pod_ip&orgId=1&refresh=5s|\tDashboard>"

            text += f":fire: `{hostname}` is down! | {url}\n"
            if last_response != "":
                text += f"```\n{last_response}\n```"

        if text != "":
            print(text)
            if params.post_to_slack:
                try:
                    client.chat_postMessage(
                        channel="#" + params.channel, text=text)
                except Exception as e:
                    logging.error(f"Failed to post slack message: {e}")
        if not params.infinite_loop:
            break

        # monitor the status of watchdog itself in slack channel
        if params.post_to_slack and checks_counter % checks_in_2_minutes == 0:
            print("Monitor watchdog...", flush=True)

            # schedule a message that will trigger if watchdog goes down without canceling it
            try:
                new_healthcheck_message_id = client.chat_scheduleMessage(
                    channel="#" + params.channel,
                    post_at=int(time.time()) +
                    params.watchdog_healthcheck_seconds,
                    text=message_watchdog_down).data["scheduled_message_id"]
            except:
                new_healthcheck_message_id = ''
                print(f"Failed to schedule slack message (down).")

            if len(healthcheck_message_id) > 0:
                # cancel previously scheduled message
                try:
                    if (not client.chat_deleteScheduledMessage(
                        channel="#" + params.channel,
                            scheduled_message_id=healthcheck_message_id).data["ok"]):
                        print(
                            f"Deleting slack message failed. id: {healthcheck_message_id}")
                except:
                    print(
                        f"Deleting slack message threw exception. id: {healthcheck_message_id}")

            healthcheck_message_id = new_healthcheck_message_id


        print("Sleeping...", flush=True)
        time.sleep(params.frequency_seconds)
        print("Woke up.", flush=True)

        checks_counter += 1


def get_args():
    obj = argparse.ArgumentParser()

    # Post to slack
    obj.add_argument('-post_to_slack',  type=bool,
                     default=os.environ.get('POST_TO_SLACK', 'false').lower() in ('true', '1', 't'))

    # Remember to always add a minimal mode to your benchmark
    obj.add_argument('-channel',  type=str,
                     default=os.environ.get('SLACK_CHANNEL', "alerts"))

    # Infinite loop
    obj.add_argument('-infinite_loop',  type=bool,
                     default=os.environ.get('INFINITE_LOOP', True))

    # Check Frequency
    obj.add_argument('-frequency_seconds',  type=int,
                     default=os.environ.get('FREQUENCY_SECONDS', 30))

    # Healthcheck of watchdog countdown in seconds
    obj.add_argument('-watchdog_healthcheck_seconds',  type=int,
                     default=os.environ.get('WATCHDOG_HEALTHCHECK_SECONDS', 600))

    # Watchdog name to distinguish if multiple watchdogs are running
    obj.add_argument('-watchdog_name',  type=str,
                     default=os.environ.get('WATCHDOG_NAME', 'default'))

    # Enable REST
    obj.add_argument('-check_rest',  type=bool,
                     default=os.environ.get('CHECK_REST', 'true').lower() in ('true', '1', 't'))

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
