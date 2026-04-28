#!/usr/bin/env bash
# Watches autoortho logs and fires `sample` the moment a background build
# semaphore hang is detected. Output saved to /tmp/aoortho_hang_sample.txt.
#
# Usage: ./tools/sample_on_hang.sh

LOG="${AUTOORTHO_LOG:-$HOME/.autoortho-data/logs/autoortho.log}"
SAMPLE_OUT=/tmp/aoortho_hang_sample.txt
SAMPLE_DURATION=10  # seconds of stack sampling

echo "Waiting for autoortho to start..."
while ! pgrep -qf "autoortho"; do sleep 1; done

AO_PID=$(pgrep -f "autoortho" | head -1)
echo "Found autoortho PID: $AO_PID"
echo "Watching log for hang indicators..."
echo "(Will run: sample $AO_PID $SAMPLE_DURATION)"

# Wait for the log file to exist
while [ ! -f "$LOG" ]; do sleep 1; done

tail -F "$LOG" | while read -r line; do
    # Trigger on a background build semaphore 30s timeout — means hang has
    # been active for at least 30s, sample immediately.
    if echo "$line" | grep -q "semaphore busy after 3000"; then
        echo ""
        echo "=== HANG DETECTED ==="
        echo "Log line: $line"
        echo "Running: sample $AO_PID $SAMPLE_DURATION -file $SAMPLE_OUT"
        sample "$AO_PID" "$SAMPLE_DURATION" -file "$SAMPLE_OUT"
        echo "Sample saved to $SAMPLE_OUT"
        echo ""
        echo "Top of stack trace:"
        head -80 "$SAMPLE_OUT"
        echo ""
        echo "Waiting for next hang..."
    fi
done
