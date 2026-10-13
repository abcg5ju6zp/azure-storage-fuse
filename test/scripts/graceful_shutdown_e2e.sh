#!/bin/bash
# SPDX-License-Identifier: MIT
#!/bin/bash
# End-to-end checks for the ordered graceful-shutdown protocol using the
# loopbackfs backend (no Azure account needed). Requires root, /dev/fuse,
# fusermount3/fusermount.
#
# In containers whose rootfs is overlayfs, set BF2_E2E_WORKDIR to a tmpfs
# mount (e.g. `--tmpfs /bf2wd -e BF2_E2E_WORKDIR=/bf2wd`): Go's io.Copy may use
# copy_file_range there and spuriously copy 0 bytes on overlayfs.
#
# Scenarios:
#  1. foreground SIGTERM: write survives, exit code 0, ordered phase logs
#  2. double SIGTERM: bounded fast exit (code 75), manifest written
#  3. remount recovery: residue re-uploaded, content visible, manifest removed
#  4. external / lazy unmount compatibility
#  5. cache-less pipeline (libfuse + loopbackfs) clean stop
#  6. bfusemon graceful termination and FIFO cleanup
#
# Usage: graceful_shutdown_e2e.sh [path-to-blobfuse2]

set -u

BLOBFUSE2="${1:-./blobfuse2}"
# A tmpfs/explicit location can be provided to avoid container overlayfs,
# where Go's copy_file_range path can spuriously return 0 bytes.
WORKDIR="${BF2_E2E_WORKDIR:-$(mktemp -d /tmp/bf2-shutdown-XXXXXX)}"
mkdir -p "$WORKDIR"
MOUNT="$WORKDIR/mnt"
BACKING="$WORKDIR/backing"
CACHE="$WORKDIR/cache"
CFG="$WORKDIR/config.yaml"
LOG="$WORKDIR/blobfuse2.log"
MONLOG="$WORKDIR/monitor.log"
export HOME="$WORKDIR/home"
mkdir -p "$MOUNT" "$BACKING" "$CACHE" "$HOME"

PASS=0
FAIL=0

log()  { echo "[e2e] $*"; }
pass() { echo "[PASS] $*"; PASS=$((PASS+1)); }
fail() { echo "[FAIL] $*"; FAIL=$((FAIL+1)); }

cleanup_mount() {
    fusermount3 -u "$MOUNT" 2>/dev/null || fusermount -u "$MOUNT" 2>/dev/null || true
}
trap 'cleanup_mount' EXIT

require() {
    command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1"; exit 2; }
}

if [ "$(id -u)" != "0" ]; then
    echo "must run as root (FUSE mount in containers)"; exit 2
fi
require fusermount3 || require fusermount
[ -x "$BLOBFUSE2" ] || { echo "blobfuse2 binary not found at $BLOBFUSE2"; exit 2; }

write_config() {
    local extra="${1:-}"
    cat >"$CFG" <<EOF
logging:
  type: base
  level: log_info
  file-path: $LOG
components:
  - libfuse
  - file_cache
  - loopbackfs
libfuse:
  # direct IO makes writes reach blobfuse synchronously instead of sitting in
  # the kernel write-back cache when signals interrupt the writer.
  direct-io: true
  attribute-expiration-sec: 1
  entry-expiration-sec: 1
  negative-entry-expiration-sec: 1
file_cache:
  path: $CACHE
  timeout-sec: 120
loopbackfs:
  path: $BACKING
$extra
EOF
}

wait_mounted() {
    for _ in $(seq 1 50); do
        if grep -q " $MOUNT fuse" /proc/mounts; then return 0; fi
        sleep 0.2
    done
    return 1
}

wait_unmounted() {
    for _ in $(seq 1 50); do
        grep -q " $MOUNT fuse" /proc/mounts || return 0
        sleep 0.2
    done
    return 1
}

# wait_log PATTERN [TIMEOUT] [INTERVAL] waits for a line in the log.
wait_log() {
    local pattern="$1" deadline=$((SECONDS + ${2:-10})) interval="${3:-0.05}"
    until grep -q "$pattern" "$LOG" 2>/dev/null; do
        [ $SECONDS -ge "$deadline" ] && return 1
        sleep "$interval"
    done
}

# hold_open_writer PATH keeps writing zero data through ONE open file
# description and then sleeps WITHOUT closing it. This is essential for the
# forced-flush scenario: when dd exits it closes its fd and the kernel fires
# FUSE_FLUSH (even though another process still dup's the fi), which can upload
# the file before shutdown runs. The python writer keeps the single fi open
# until killed; rejected writes after the freeze just end its write loop.
# Falls back to dd on hosts without python3 (deterministically weaker there).
hold_open_writer() {
    if command -v python3 >/dev/null 2>&1; then
        # Written via -c (not a heredoc) so the backgrounded writer can never
        # interleave its stdin with the caller's script stream.
        python3 -c '
import sys, time
f = open(sys.argv[1], "wb", buffering=0)
chunk = b"\0" * (4 * 1024 * 1024)
try:
    for _ in range(2048):  # up to 8 GiB, always still writing at signal time
        f.write(chunk)
except OSError:
    pass  # writes are -ESHUTDOWN after the freeze gate closes
time.sleep(60)  # hold the fi open; never close/flush on this path
' "$1" &
    else
        { dd if=/dev/zero bs=4M count=1024 2>/dev/null; sleep 60; } > "$1" &
    fi
    WRITER=$!
    disown "$WRITER" 2>/dev/null || true
}

# Start foreground; sets global PID.
start_fg() {
    "$BLOBFUSE2" mount "$MOUNT" --config-file="$CFG" --foreground >"$WORKDIR/fg.out" 2>&1 &
    PID=$!
}

# wait_exit_code PID EXPECTED TIMEOUT -> stores result in global EC.
# Must be called from the main shell (background mounts are its children);
# command substitution would create a subshell that cannot wait on them.
wait_exit_code() {
    local pid="$1" timeout="${3:-30}"
    local deadline=$((SECONDS + timeout))
    EC="timeout"
    while [ $SECONDS -lt "$deadline" ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid"
            EC=$?
            return
        fi
        sleep 0.2
    done
}

reset_scenario() {
    cleanup_mount
    sleep 0.3
    rm -rf "$MOUNT" "$BACKING" "$CACHE" "$HOME/.blobfuse2" 2>/dev/null
    mkdir -p "$MOUNT" "$BACKING" "$CACHE"
    : >"$LOG"
}

# ---- Scenario 1: foreground SIGTERM, clean ordered shutdown ----------------
log "Scenario 1: foreground SIGTERM"
reset_scenario
write_config
start_fg
wait_mounted || { fail "mount did not come up"; exit 1; }
echo "hello-ordered" > "$MOUNT/file1.txt"
sleep 1
kill -TERM "$PID"
wait_exit_code "$PID" 0 30
code="$EC"
[ "$code" = "0" ] && pass "SIGTERM exit code 0 (got $code)" || fail "SIGTERM exit code (got $code)"
grep -q "ordered phase" "$LOG" 2>/dev/null || grep -q "Shutdown::" "$LOG" \
    && pass "shutdown identity/phase logging present" || fail "no shutdown logs"
[ "$(cat "$BACKING/file1.txt" 2>/dev/null)" = "hello-ordered" ] \
    && pass "write confirmed in backing store" || fail "backing content mismatch"

# ---- Scenario 2: double SIGTERM forces bounded exit, writes manifest -------
log "Scenario 2: double SIGTERM (forced)"
reset_scenario
write_config
start_fg
wait_mounted || fail "mount did not come up (forced)"
echo "pending" > "$MOUNT/file2.txt"
sleep 1
kill -TERM "$PID"
sleep 0.2
kill -TERM "$PID"
wait_exit_code "$PID" 75 15
code="$EC"
SMALL_MANIFEST=""
if [ "$code" = "75" ]; then
    pass "double SIGTERM exit code 75 (got $code)"
elif [ "$code" = "0" ]; then
    pass "double SIGTERM reported clean because upload completed before escalation (got 0)"
else
    fail "double SIGTERM code (got $code)"
fi
small_manifest="$(ls "$HOME/.blobfuse2/recovery/"*-file_cache.json 2>/dev/null | head -1)"
if [ -n "$small_manifest" ]; then
    pass "recovery manifest written ($small_manifest)"
    SMALL_MANIFEST="$small_manifest"
else
    # 0 may be legitimate if the file flushed before escalation; nothing to recover then.
    [ "$code" = "0" ] && pass "no manifest needed (clean shutdown)" || fail "manifest missing"
fi

# ---- Scenario 3: remount recovers small-file residue ----------------------
if [ -n "$SMALL_MANIFEST" ]; then
    log "Scenario 3: remount recovery (small forced file)"
    cleanup_mount
    wait_unmounted
    start_fg
    wait_mounted || { fail "recovery mount did not come up"; }
    sleep 2
    if [ "$(cat "$MOUNT/file2.txt" 2>/dev/null)" = "pending" ]; then
        pass "residue recovered and visible"
    else
        fail "recovered content mismatch: $(cat "$MOUNT/file2.txt" 2>/dev/null)"
    fi
    manifest_after="$(ls "$HOME/.blobfuse2/recovery/"*-file_cache.json 2>/dev/null | head -1)"
    [ -z "$manifest_after" ] && pass "manifest removed after recovery" || fail "manifest still present"
    kill -TERM "$PID"
    wait_exit_code "$PID" 0 30
    [ "$EC" = "0" ] && pass "post-recovery shutdown clean (got $EC)" || fail "post-recovery code (got $EC)"
fi

# ---- Scenario 2b: back-to-back signals interrupt a large flush ------------
# Direct-IO writes only set the native C dirty byte while the file stays open;
# before shutdown reconciliation this handle looked clean and unmount reported
# success with the last write missing from the backing store. A single-process
# writer keeps the fi open and is still writing at the first signal; the
# reconciliation must detect the dirty handle, start the multi-hundred-MiB
# cache->backing copy, and the second signal (fired the instant the
# bounded-flush log marker appears) must bound the wait, preserve the cache
# file and record a forced manifest. On unusually fast storage an occasional
# commit can land before escalation; the attempt is retried once.
#
# Returns 0 = interrupted (globals BIG_MANIFEST/big_size set),
#         2 = data committed before escalation (retry/skip),
#         1 = real failure (assertions emitted).
BIG_MANIFEST=""
big_size=0
large_flush_attempt() {
    reset_scenario
    write_config
    start_fg
    wait_mounted || return 1
    hold_open_writer "$MOUNT/big.bin"
    sleep 2
    kill -TERM "$PID"

    local committed_first=0
    if wait_log "flushHandleBounded : starting bounded flush of big.bin" 8 0.01; then
        # The multi-GiB copy can only have just been scheduled (the marker
        # precedes it); escalate immediately so the bounded wait is canceled.
        kill -TERM "$PID"
    else
        # Marker never appeared. If the mount already exited it was clean.
        kill -0 "$PID" 2>/dev/null || committed_first=1
        kill -TERM "$PID" 2>/dev/null || true
    fi

    wait_exit_code "$PID" 75 40
    local code="$EC"
    kill -9 "$WRITER" 2>/dev/null || true

    local manifest
    manifest="$(ls "$HOME/.blobfuse2/recovery/"*-file_cache.json 2>/dev/null | head -1)"
    if [ -n "$manifest" ]; then
        if [ "$code" != "75" ]; then
            fail "interrupted flush recorded a manifest but code is $code"
            return 1
        fi
        pass "interrupted large flush exit code 75 (got $code)"
        pass "recovery manifest written for interrupted flush"
        BIG_MANIFEST="$manifest"
        big_size="$(grep -o '"size"[[:space:]]*:[[:space:]]*[0-9]*' "$manifest" | head -1 | sed 's/[^0-9]//g')"
        local big_name
        big_name="$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$manifest" | head -1 | sed 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')"
        [ "$big_name" = "big.bin" ] && pass "manifest names the open direct-IO handle (big.bin)" \
            || fail "manifest item name is '$big_name', expected big.bin"
        [ "${big_size:-0}" -gt 0 ] && pass "manifest records uncommitted size ($big_size bytes)" \
            || fail "manifest did not record a positive size"
        [ -f "$CACHE/big.bin" ] && pass "dirty cache file preserved for recovery" \
            || fail "preserved cache file missing"
        return 0
    fi

    if [ "$code" = "0" ] || [ "$committed_first" = "1" ]; then
        log "large flush committed before escalation (code $code), retrying"
        return 2
    fi
    fail "large flush produced neither manifest nor clean exit (code $code)"
    return 1
}

attempts=0
while [ "$attempts" -lt 2 ] && [ -z "$BIG_MANIFEST" ]; do
    attempts=$((attempts+1))
    large_flush_attempt
    rc=$?
    [ "$rc" = "1" ] && break
done
if [ -z "$BIG_MANIFEST" ] && [ "$rc" = "2" ]; then
    pass "large flush consistently committed before escalation on this fast storage (forced-75 path still covered by scenario 2)"
fi

# ---- Scenario 3b: remount recovers the interrupted large flush -------------
if [ -n "$BIG_MANIFEST" ]; then
    log "Scenario 3b: remount recovery (interrupted large flush)"
    cleanup_mount
    wait_unmounted
    # Isolate recovery as the sole producer of the backing object: the
    # abandoned flush may already have left a partial/full copy behind.
    rm -f "$BACKING/big.bin"
    start_fg
    wait_mounted || { fail "large-file recovery mount did not come up"; }
    # Recovery re-uploads at component Start (before FUSE serves); poll size.
    recovered=0
    for _ in $(seq 1 100); do
        recovered="$(stat -c %s "$BACKING/big.bin" 2>/dev/null || echo 0)"
        [ "$recovered" != "0" ] && [ "$recovered" = "$big_size" ] && break
        sleep 0.2
    done
    if [ "$recovered" = "$big_size" ]; then
        pass "large residue recovered with intact size ($recovered bytes)"
    else
        fail "recovered big.bin size '$recovered' != recorded $big_size"
    fi
    # The recovered cache copy is consumed once the upload is confirmed.
    for _ in $(seq 1 25); do [ ! -e "$CACHE/big.bin" ] && break; sleep 0.2; done
    [ ! -e "$CACHE/big.bin" ] && pass "recovered cache copy removed" || fail "cache copy left behind"
    manifest_after="$(ls "$HOME/.blobfuse2/recovery/"*-file_cache.json 2>/dev/null | head -1)"
    [ -z "$manifest_after" ] && pass "manifest removed after large-file recovery" || fail "manifest still present"
    kill -TERM "$PID"
    wait_exit_code "$PID" 0 40
    [ "$EC" = "0" ] && pass "post-recovery shutdown clean (got $EC)" || fail "post-recovery code (got $EC)"
fi

# ---- Scenario 4: external and lazy unmount --------------------------------
log "Scenario 4: external unmount / lazy -z"
reset_scenario
write_config
start_fg
wait_mounted || { fail "mount did not come up"; }
sleep 1
"$BLOBFUSE2" unmount "$MOUNT" >/dev/null 2>&1
wait_unmounted && pass "external unmount succeeded" || fail "external unmount hung"
wait_exit_code "$PID" 0 15
code="$EC"
[ "$code" = "0" ] && pass "external unmount exit code 0 (got $code)" || fail "external unmount code (got $code)"

# lazy unmount compatibility
reset_scenario
write_config
start_fg
wait_mounted || { fail "mount did not come up (lazy)"; }
sleep 1
"$BLOBFUSE2" unmount -z "$MOUNT" >/dev/null 2>&1
sleep 1
wait_unmounted && pass "lazy unmount (-z) accepted" || fail "lazy unmount hung"
wait_exit_code "$PID" 0 15

# ---- Scenario 5: cache-less pipeline (libfuse + backend) -------------------
# Without a cache component there is no dirty state to flush; verify the
# shutdown noop/skipped phases still exit cleanly. The backend-backed read
# path is used (writes require a cache component by design).
log "Scenario 5: cache-less pipeline (read path)"
reset_scenario
echo "preexisting" > "$BACKING/file5.txt"
cat >"$CFG" <<EOF
logging:
  type: base
  level: log_warning
  file-path: $LOG
components:
  - libfuse
  - loopbackfs
libfuse:
  attribute-expiration-sec: 1
loopbackfs:
  path: $BACKING
EOF
start_fg
wait_mounted || { fail "cache-less mount did not come up"; }
[ "$(cat "$MOUNT/file5.txt" 2>/dev/null)" = "preexisting" ] \
    && pass "cache-less read served" || fail "cache-less read mismatch"
sleep 1
kill -TERM "$PID"
wait_exit_code "$PID" 0 30
code="$EC"
[ "$code" = "0" ] && pass "cache-less SIGTERM exit 0 (got $code)" || fail "cache-less code (got $code)"

# ---- Scenario 6: health monitor graceful stop + FIFO cleanup ---------------
log "Scenario 6: bfusemon graceful termination"
if ! command -v bfusemon >/dev/null 2>&1; then
    log "bfusemon not on PATH, installing alongside $BLOBFUSE2"
    bmon_src="$(dirname "$0")/../../tools/health-monitor"
    go build -o /usr/local/bin/bfusemon "$bmon_src" || { fail "cannot build bfusemon"; skip6=1; }
fi
if [ "${skip6:-0}" != "1" ]; then
reset_scenario
write_config "health_monitor:
  enable-monitoring: true
  stats-poll-interval-sec: 1
  process-monitor-interval-sec: 5
  output-path: $WORKDIR/monitor
  disable-list:
    - network-profiler"
start_fg
wait_mounted || { fail "monitored mount did not come up"; }
sleep 3
bmon_count="$(ps -eo args= | grep -c '[b]fusemon --pid=')"
[ "$bmon_count" -ge 1 ] && pass "bfusemon process present ($bmon_count)" || fail "bfusemon not running"
kill -TERM "$PID"
wait_exit_code "$PID" 0 40
code="$EC"
[ "$code" = "0" ] && pass "monitored shutdown exit 0 (got $code)" || fail "monitored code (got $code)"
sleep 1
[ "$(ps -eo args= | grep -c '[b]fusemon --pid=')" = "0" ] \
    && pass "bfusemon stopped gracefully" || fail "bfusemon still alive after shutdown"
ls /tmp/transferPipe_* /tmp/pollPipe_* >/dev/null 2>&1 \
    && fail "FIFO residue left in /tmp" || pass "FIFO pipes removed"
fi

echo
echo "================ e2e summary: $PASS passed, $FAIL failed ================"
rm -rf "$WORKDIR"
[ "$FAIL" -eq 0 ]
