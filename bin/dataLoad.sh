#!/bin/sh
wget -i ./download_links.txt

for f in *.gz; do
  STEM=$(basename "${f}" .gz)
  gunzip -c "${f}" > ./data/"${STEM}"
done

hadoop fs -test -d /user/ndulam/weather_raw

if [$? == 0]; then
    echo "dir exists"
else
hadoop fs -mkdir -p /user/ndulam/weather_raw
fi

#hadoop fs -copyFromLocal ./data/* /user/ndulam/weather_raw/

beeline -u jdbc:hive2://localhost:10000/default -n cloudera -p cloudera hive -f ./queries.hql

