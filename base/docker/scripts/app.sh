
echo "This is the app.sh file from aperturedata/workflows-base"
echo "You have to implement me. You did not."

STAY_ALIVE=$(/app/wf_argparse.py --type bool --envar STAY_ALIVE --default false)

if [ "${STAY_ALIVE}" = "true" ]; then
    echo "STAY_ALIVE is set to true. I will not exit."
    # This loop will run indefinitely.
    while true; do
        sleep 60
    done
fi

echo "I did nothing. Bye."
