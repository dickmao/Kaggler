#!/bin/bash -e

function usage() {
   echo "Usage: $0 GCSPATH MNTPATH [key-file]"
   exit 1
}

GCSPATH=${1:-}
if [ -z $GCSPATH ]; then
    usage
fi

MNTPATH=${2:-}
if [ -z $MNTPATH ]; then
    usage
fi
if [ ! -d "$(dirname $MNTPATH)" ]; then
    echo "$(dirname $MNTPATH)" does not exist
fi

KEYFILE=${3:-}
if [ ! -z $KEYFILE ]; then
    KEYFILE=" --key-file $KEYFILE"
fi

mkdir -p $MNTPATH
fusermount -qu $MNTPATH || true
if mountpoint -q -- $MNTPATH ; then
    echo "**" $MNTPATH still mounted
    exit -1
fi
gcsfuse --implicit-dirs$KEYFILE $GCSPATH $MNTPATH
if ! mountpoint -q -- $MNTPATH ; then
    echo "**" gcsfuse failed
    exit -1
fi
if ! ls $MNTPATH >& /dev/null ; then
    echo "**" Unreadable: $MNTPATH unreadable
    echo "**" Try,
    echo "**" gcloud auth application-default login, or
    echo "**" Pass service-account json as third argument
    echo "**" Set the environment variable GOOGLE_APPLICATION_CREDENTIALS
    exit -1
fi
