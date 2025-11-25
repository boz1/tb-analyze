#!/bin/bash

# Loop over all days in August 2025
for day in {01..31}; do

    DATE="2025/08/$day"
    DATEFOLDER="2025-08-$day"

    SRC="s3://timeboost-auctioneer-arb1/ue2/validated-timeboost-bids/$DATE/"
    DST="/c/Users/ozbur/Documents/Burak/research/Timeboost/tb-analyze/data/bids/$DATEFOLDER"

    echo "------------------------------------------------------------"
    echo "Downloading Timeboost bids for $DATE into $DST"
    echo "------------------------------------------------------------"

    mkdir -p "$DST"

    # download files for the day
    aws s3 cp "$SRC" "$DST" --recursive --no-sign-request

    echo "Unzipping .gzip files for $DATE ..."
    for f in "$DST"/*.gzip; do
        echo "Unzipping $f"
        gzip -dc "$f" > "${f%.gzip}"    # force-decompress even with .gzip extension
        rm "$f"
    done

    echo "Finished $DATE"
    echo ""

done

echo "🎉 All August files downloaded & unzipped!"
