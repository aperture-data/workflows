
echo "This is the app.sh file from aperturedata/workflows-base"
echo "You have to implement me. You did not."

# if STAY_ALIVE is set to true, the app should not exit.
if [ -z "${STAY_ALIVE}" ]; then
    STAY_ALIVE=false
fi
if [ "$STAY_ALIVE" == true ]; then
    echo "STAY_ALIVE is set to true. I will not exit."
    while true; do
        sleep 60
    done
fi

echo "I did nothing. Bye."
