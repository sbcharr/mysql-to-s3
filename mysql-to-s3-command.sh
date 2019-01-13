#!/bin/bash

# $1 represents table to be processed
# $2 represents load type, full or incr
# $3 represents execution date in yyyy-mm-dd format
# $4 represents log level

START=$(date +%s)

AWS_PROFILE="squeakysimple"

if [ $2 == "full" ]; then
    OUTDIR="./out/$1"
    S3_DEST="s3://data-load-squeakysimple/$1/"
else
    year="$(cut -d'-' -f1 <<< $3)"
    month="$(cut -d'-' -f2 <<< $3)"
    day="$(cut -d'-' -f3 <<< $3)"

    OUTDIR="./out/$1/${year}/${month}/${day}"
    S3_DEST="s3://data-load-squeakysimple/$1/${year}/${month}/${day}/"
fi


if [ ! -d ${OUTDIR} ]; then
    mkdir -p ${OUTDIR}
else
    rm -f ${OUTDIR}/*
fi

if [ $? -ne 0 ]; then
    echo "ERROR unable to process the output folder"
    exit 1
fi

START2=$(date +%s)
python3 ./src/mysql_to_s3.py $1 $2 $3 $4
if [ $? -ne 0 ]; then
    echo "ERROR failed to run the python script"
    exit 1
fi

END2=$(date +%s)
DIFF2=$(( $END2 - $START2 ))

echo "The python job took $DIFF2 seconds"

# archive into .gzip format
for FILE in `ls ${OUTDIR}/`; do
    gzip ${OUTDIR}/${FILE}
    if [ $? -ne 0 ]; then
        echo "ERROR unable to archive the file"
        exit 1
    fi
done

# upload to s3
aws s3 sync ${OUTDIR}/ ${S3_DEST} --delete --profile $AWS_PROFILE >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR upload to s3 failed"
    exit 1
fi

rm -f ${OUTDIR}/*
if [ $? -ne 0 ]; then
    echo "ERROR unable to clean the output folder"
    exit 1
fi

END=$(date +%s)
DIFF=$(( $END - $START ))

echo "The job took $DIFF seconds"



