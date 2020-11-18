#!/bin/bash -e

function usage() {
   echo "Usage: $0 [-c competition] [-d dataset]"
   exit 1
}

while [[ $# -gt 0 ]] ; do
  key="$1"
  case "$key" in
      -d)
      DATASET=$2
      shift
      shift
      ;;
      -c)
      COMPETITION=$2
      shift
      shift
      ;;
      *)
      break
      ;;
  esac
done

if [ -z $COMPETITION ] && [ -z $DATASET ] ; then
    usage
fi
if [ -z $DATASET ] ; then
    BASE_DATASET=$(basename $COMPETITION)
    DATASET_SOURCES="[]"
else
    BASE_DATASET=$(basename $DATASET)
    DATASET_SOURCES="[\"${DATASET}\"]"
fi
if [ -z $COMPETITION ] ; then
    COMPETITION_SOURCES="[]"
else
    COMPETITION_SOURCES="[\"${COMPETITION}\"]"
fi

USER=$(kaggle config view | grep -o "username: .*" | cut -d' ' -f2)
if [ -z $USER ]; then
    echo Kaggle user indeterminate
    exit 1
fi
wdir=$(mktemp -d -t gcspath-XXXXXXX)
pushd $wdir

cat <<EOF >"gcspath.py"
from kaggle_datasets import KaggleDatasets
print(KaggleDatasets().get_gcs_path('${BASE_DATASET}'))
EOF

cat <<EOF >"kernel-metadata.json"
{
  "id": "${USER}/gcspath",
  "title": "gcspath",
  "code_file": "gcspath.py",
  "language": "python",
  "kernel_type": "script",
  "is_private": "true",
  "enable_gpu": "false",
  "enable_internet": "true",
  "dataset_sources": ${DATASET_SOURCES},
  "competition_sources": ${COMPETITION_SOURCES},
  "kernel_sources": []
}
EOF
if ! kaggle k push -p . | tee ./push.out | grep -q "success" ; then
    cat ./push.out
else
    fail=""
    for f in {1..120} ; do
        if [ $f = "120" ]; then
            echo Timed out
            fail="1"
            break
        fi
        STATUS=$(kaggle k status "${USER}/gcspath")
        if [ $? -ne 0 ]; then
            fail="1"
            break
        fi
        if echo $STATUS | grep -o "status .*" | grep -w -q -E "complete|error" ; then
            break
        fi
        echo $STATUS | grep -o "status .*"
        sleep 1
    done
    if [ -z "$fail" ] && kaggle k output -q "${USER}/gcspath" ; then
        cat gcspath.log | grep -o "gs:[/a-z0-9-]*"
    fi
fi
popd
rm -rf $wdir
