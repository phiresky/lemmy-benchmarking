#!/bin/bash

# bracket to prevent mangling on file edit
{
    # make bash a bit less stupid
    set -euo pipefail
    IFS=$'\n\t'

    cd $(dirname $0)

    sourcedir=../lemmy
    outroot=results/runs
    runname="$1"
    run="$(date -Is)-$runname"
    rundir=$outroot/$run
    flamegraph=~/.local/lib/FlameGraph
    if [[ ! -f $flamegraph/flamegraph.pl ]]; then
        echo flamegraph not found in $flamegraph
        exit 1
    fi

    pushd $sourcedir
    cargo build --release
    popd
    cargo build --release

    echo "run: $run"
    export DOCKER_USER=$(id -u):$(id -g)
    docker compose down
    rm -rf volumes/postgres
    mkdir -p volumes/postgres
    docker compose up postgres --wait

    mkdir -p $rundir
    perffile=$rundir/perf.data
    export LEMMY_CONFIG_LOCATION=$PWD/lemmy-config.hjson

    RUST_LOG=warn LEMMY_ALLOW_HTTP=1 \
        $sourcedir/target/release/lemmy_server >$rundir/server.log &
    SERVERPID=$!
    perf record --freq=99 --output $perffile --call-graph dwarf --pid $SERVERPID &
    PERFPID=$!
    trap "kill $SERVERPID" EXIT

    while ! curl localhost:8536/api/v3/site >/dev/null 2>&1; do
        echo waiting for server up
        sleep 1
    done
    echo server up

    RUST_LOG=warn target/release/dump_uploader --remote-server http://lemmy.localhost:8536/inbox \
        --server-pid $SERVERPID \
        --input-file data/Import-Reddit-Dump-v1/dump.jsonl.zst --output-json $rundir/info.json \
        --runname $runname \
        >$rundir/upload.log

    sleep 3
    kill $SERVERPID 2>/dev/null || true
    # remove trap
    trap - EXIT

    wait $SERVERPID || true
    wait $PERFPID || true

    perfscript=$perffile.txt
    perf script -i $perffile -F +pid >$perfscript
    $flamegraph/stackcollapse-perf.pl $perfscript | $flamegraph/flamegraph.pl --title "$run" --subtitle lemmy_server --hash >$rundir/perf.data.flamegraph.svg
    # perf data convert -i $perffile --to-json $perffile.json
    exit
}
