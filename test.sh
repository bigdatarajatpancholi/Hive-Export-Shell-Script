#ter_nbr int,calendar_month_in_year_nbr int,calendar_month_day_qty int,calendar_month_begin_dt timestamp,calendar_month_end_dt timestamp,calendar_month_sequence_n$
#CREATE EXTERNAL TABLE kaushal.kothari_day ($(fields))
#        ROW FORMAT DELIMITED
#        FIELDS TERMINATED BY ','
#        STORED AS TEXTFILE
#        LOCATION '/user/bigdatarajatpancholi1035/day_out';
export_stmt="$2"
echo "$export_stmt"
DBTBL="$( echo "$export_stmt" | sed -E -e "s/^.*?from//gi" | awk '{print $1}' )"
echo "$DBTBL"
hived_host=$1
echo "$hived_host"
hive_conn_string=$1
filename=$3
echo "$filename"
delimiter=$4
echo "$delimiter"
information_area=$5
echo "$information_area"
if [[ $DBTBL == *"."* ]]
    then
      database="$( echo "$DBTBL" | awk -F. '{print $1}')"
      table="$( echo "$DBTBL" | awk -F. '{print $2}')"
      echo ""
      echo "DATABASE: $database"
      echo "TABLE: $table"
      echo ""
    else
	
	 echo "Please use database.table in your sql statement."
      exit 1
    fi
location=`beeline -u $hive_conn_string -e "use ${database}; DESCRIBE FORMATTED ${table}" | grep 'Location:' | cut -d \| -f 3 | sed 's,/*[^/]\+/*$,,' | xargs`
    echo "location= $location"
    hdfs_out_location=$location/text_out
create=`hive --silent=true  -e "show create table ${database}.${table}"`
#echo "prepare_stmt create=$create"
trimmed=${create//+}
#echo "prepare_stmt trimmed1=$trimmed"
trimmed=${trimmed//-}
echo "prepare_stmt trimmed2=$trimmed"
trimmed=${trimmed//|}
echo "$trimmed" >> temp1.txt
cat temp1.txt | cut -d "(" -f2  >> temp2.txt
cat temp2.txt | sed -e "/ROW FORMAT DELIMITED/,\$d" >> temp3.txt
#cat temp3.txt |grep "\`*\`"|sed "s/*//g"|sed "s/\`//g"|tr '\n' ',' >> temp4.txt
#cat temp3.txt |sed 's/ [a-z]*, /,/g' >> temp4.txt
cat temp3.txt |awk -F " " '{print $1,$2}' >> temp5.txt
#cat temp5.txt |grep "\`*\`"|sed "s/*//g"|sed "s/\`//g"|tr '\n' ','|sed 's/,$//'>> temp6.txt
cat temp5.txt |sed -e "s/)$//"|sed -e "s/\`//g"|xargs >> temp6.txt
#fields= `cat temp6.txt`
fields=$( cat temp6.txt )
echo $fields
echo "Starting of create_table"
beeline -u $hive_conn_string -e "DROP TABLE IF EXISTS ${database}.${table}_text_out;CREATE EXTERNAL TABLE ${database}.${table}_text_out (${fields})
        ROW FORMAT DELIMITED
		 FIELDS TERMINATED BY '${delimiter}'
        STORED AS TEXTFILE
        LOCATION '${hdfs_out_location}'"  >> ./create_insert_table.txt
echo "Ending of create_table"
echo "Starting of Insert Statement"
beeline -u $hive_conn_string -n bigdatarajatpancholi1035 -p 2Y7SWMYP -e "INSERT INTO TABLE ${database}.${table}_text_out ${export_stmt}"   >> ./insert_stmt.txt
echo "Ending of Insert Statement"

filename="/home/bigdatarajatpancholi1035/staging/${information_area}/out/${filename}"
		
		echo "filename= $filename"

        hdfs dfs -getmerge $hdfs_out_location ${filename}_tmp.csv

        echo $header > ${filename}.csv
        cat ${filename}_tmp.csv >> ${filename}.csv

        rm ${filename}_tmp.csv
