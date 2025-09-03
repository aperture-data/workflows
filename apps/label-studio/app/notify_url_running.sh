#notify_url_running.sh - notifies that url is avaiable.

# Run in our venv
echo "starting" >> /tmp/update.log
. /opt/venv/bin/activate
python3 /app/status_tools.py --phase "serving" --completed 100 --accessible
echo "done" >> /tmp/update.log
