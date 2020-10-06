#!/bin/bash

function usage {
	cat <<EOM
   Usage: $(basename "$0") [OPTION]...

  -i    path to input csv file
        example: ./data/sample/sample.part1.csv
        the csv file should NOT contain a header

  -h    display help
EOM
	exit 2
}


while getopts ":i:h" optKey; do
	case "$optKey" in
		i)
			i=$OPTARG
			;;
		h|*)
			usage
			;;
	esac
done

shift $((OPTIND - 1))

if [[ -z "$i" ]]; then
    echo "Must provide input csv location"
    usage 1>&2
    exit 1
fi

echo "clean output location...."

rm -rf ./tmp/data

echo "creating tmp dir for result data..."
mkdir -p ./tmp/data/


echo "executing mock-up pipelines..."

python dry_run.py -i=$i

echo "output dir is ./tmp/data/event_agg.csv..."
