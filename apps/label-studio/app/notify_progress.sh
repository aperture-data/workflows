#notify_progress.sh - notifies that processing is happening

if [ $# -lt 1 ]; then
    echo "Need argument to notify"
    exit 1
fi

# Run in our venv
echo "starting" >> /tmp/update.log
. /opt/venv/bin/activate
python3 /app/status_tools.py --completed $1 --accessible
echo "done" >> /tmp/update.log
