#!/bin/bash -e

script_name="j800_execute_hive_export_table.sh"

# Functions
function confirm_parameters {
    if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ]; then
        parameters_ok="false"
        print_usage
    fi
}

function set_parameters {
    hived_host=$1
    hive_conn_string="jdbc:hive2://ip-172-31-38-146.ec2.internal:2181,ip-172-31-35-141.ec2.internal:2181,ip-172-31-20-247.ec2.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
    export_stmt="$2"
    filename=$3
    delimiter=$4
    information_area=$5

    DBTBL="$( echo "$export_stmt" | sed -E -e "s/^.*?from//gi" | awk '{print $1}' )"
	echo "$DBTBL"

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
    hdfs_out_location=$location/out
}

function print_usage {
    echo ""
    echo "-----------------------------------"
    echo "| $script_name USAGE |"
    echo "-----------------------------------"
    echo ""
    echo "  INFO:"
    echo "    This script will execute Hive, via"
    echo "    Beeline, against an HQL script, first"
    echo "    setting up the environment as needed."
    echo "    It will also compute stats on the"
    echo "    affected table."
    echo ""
    echo "  SYNTAX:"
    echo "    $script_name"
    echo "      [hive server 2 host]"
    echo "      [export statement]"
    echo "      [filename]"
    echo "      [field delimiter]"
    echo "      [information_area]"
    echo ""
    echo "  EXAMPLES:"
    echo "    $script_name hived.XXX.com \"select * from raw_marketing.double_click_ad_impression\" \"network_impression.log\" \"|\" \"marketing\""
    echo ""
    echo "  See repository README.md for more info."
    echo ""
}

function prepare_stmt {
    export_stmt="$1"
    create=`hive --silent=true  -e "show create table ${database}.${table}"`
    #echo "prepare_stmt create=$create"
    trimmed=${create//+}
    #echo "prepare_stmt trimmed1=$trimmed"
    trimmed=${trimmed//-}
    #echo "prepare_stmt trimmed2=$trimmed"
    trimmed=${trimmed//|} 
    echo "$trimmed" >> temp1.txt
    #echo "prepare_stmt trimmed3=$trimmed"
   
    #fields=`echo $trimmed | cut -d "(" -f2 | cut -d ")" -f1|cut -d "'" -f2`
    #echo "prepare_stmt fields1=$fields"
    #fields=${fields//\`}
    #echo "prepare_stmt fields2=$fields"

	# fields=`echo $trimmed | cut -d "(" -f2 | sed -e "/ROW FORMAT SERDE/,\$d" | sed 's/ [a-z]*, /,/g' | sed 's/ [a-z]*//g' |  sed "s/,/$delimiter/g" | cut -d "'" -f1 | sed -e "s/COMMENT//g" | cut -d "\`" -f2`
	
	cat temp1.txt | cut -d "(" -f2  >> temp2.txt				
	cat temp2.txt | sed -e "/ROW FORMAT SERDE/,\$d" >> temp3.txt	
	#cat temp3.txt |grep "\`*\`"|sed "s/*//g"|sed "s/\`//g"|tr '\n' ',' >> temp4.txt
	cat temp3.txt |sed 's/ [a-z]*, /,/g' >> temp4.txt
	cat temp4.txt |awk -F " " '{print $1,$2}' >> temp5.txt
	cat temp5.txt |grep "\`*\`"|sed "s/*//g"|sed "s/\`//g"|tr '\n' ','|sed 's/,$//'>> temp6.txt
	#fields= `cat temp6.txt`
	fields=$( cat temp6.txt )
	echo $fields

	
	#cat temp3.txt |  sed 's/ [a-z]*, /,/g' | sed 's/ [a-z]*//g'>> temp4.txt	
	#cat temp4.txt |  sed "s/,/$delimiter/g" >>temp5.txt
	#fields= cat temp5.txt | cut -d "'" -f1 | sed -e "s/COMMENT//g" | cut -d "\`" -f2 | sed '/)/d' | xargs | sed -e #'s/ /,/g'
	
	
	#hdfs dfs -rm -r temp*.txt
	
    echo "${export_stmt}" | grep --quiet "*"
    if [ $? = 1 ]
    then
        echo "=====> no *"
        OUTPUT=""
        COLUMN_LIST="$( echo "$export_stmt" | sed -E -e "s/select //gi" | sed -E -e "s/ from .+//gi")"
        echo $COLUMN_LIST
        # Remove the commas in the column list to get a space separated list of columns.
        for COLUMN_NAME in $( echo "$COLUMN_LIST" | sed -E -e "s/,/ /g" ); do
          echo "Looking up type for $COLUMN_NAME"
                
          # We'll assume the case matches and all that.  From the Fields variable, just remove anything not matching pattern "COLUMN_NAME TYPE"
          # Save the result into an output variable.
           OUTPUT="$OUTPUT $(echo "${COLUMN_NAME} string,")" # | sed -E -e "s/.*($COLUMN_NAME [^ ]+).*/\1/g")"          
        #  OUTPUT="$OUTPUT $(echo "$fields" | sed -E -e "s/.*($COLUMN_NAME [^ ]+).*/\1/g")"                  
        done
        echo $OUTPUT

        # Being finicky, remove the starting space and trailing comma.
        OUTPUT="$( echo "$OUTPUT" | sed -E -e "s/^ //g" | sed -E -e "s/,$//g")"

        fields=$OUTPUT
    fi

    #header=`echo $fields | sed 's/ [a-z]*, /,/g' | sed 's/ [a-z]*//g'`
	#echo "header= $header"
	
	echo "header= $fields"
	
   # echo ${create} | grep --quiet " PARTITIONED BY "
   #if [ $? = 0 ]
   #then
   #       echo "=====> PARTITIONED TABLE"
   #      export_stmt=`echo "$export_stmt" | sed #'s/\*/'"$header"'/g'`
	#  echo "export_stmt= $export_stmt"
    #fi
    
	echo "End of the prepare_stmt function"
	
    #set the delimiter in the header
    #header=`echo $header | sed "s/,/$delimiter/g"`
    #echo "header = $header"
}

#Execution
confirm_parameters "$1" "$2" "$3" "$4" "$5"

if [ -z "$parameters_ok" ]; then
        #set parameters
        set_parameters "$1" "$2" "$3" "$4" "$5"

        #clean hdfs temp location
        #hdfs dfs -rm -r $hdfs_out_location/*

        #get the fields and header
        prepare_stmt "${export_stmt}"

		
		
		#build create and execute statement
        #create_table="DROP TABLE IF EXISTS ${database}.${table}_out;
        #CREATE EXTERNAL TABLE ${database}.${table}_out (${fields})
        #ROW FORMAT DELIMITED
        #FIELDS TERMINATED BY '${delimiter}'
        #STORED AS TEXTFILE
        #LOCATION '${hdfs_out_location}'"
        
		
		
        #export_stmt="INSERT INTO TABLE ${database}.${table}_out {export_stmt}"
        #echo "export_stmt in execution = $export_stmt"
        
		#execute the statements
				
        #beeline -u $hive_conn_string -e "${create_table}"  >> ./create_table.txt
		
		echo "Starting of create_table"
		
		beeline -u $hive_conn_string -e "DROP TABLE IF EXISTS ${database}.${table}_out;CREATE EXTERNAL TABLE ${database}.${table}_out (${fields})
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '${delimiter}'
        STORED AS TEXTFILE
        LOCATION '${hdfs_out_location}'"  >> ./create_table.txt
		
		echo "Ending of create_table"
		
		echo "Starting of Insert Statement"
		
        beeline -u $hive_conn_string -e "INSERT INTO TABLE ${database}.${table}_out {export_stmt}"   >> ./insert_stmt.txt
		
		echo "Ending of Insert Statement"
		
        filename="/opt/gmi/bd_userapps/staging/${information_area}/out/${filename}"
		
		echo "filename= $filename"

        hdfs dfs -getmerge $hdfs_out_location ${filename}_tmp.csv

        echo $header > ${filename}.csv
        cat ${filename}_tmp.csv >> ${filename}.csv

        rm ${filename}_tmp.csv
        
fi