# data_intensive_architecture

In the project I used Google Health's COVID-19 Open Data Repository to analyze realionship between age and gender of a patient with Covid-19 infection.The objective of the project was to answer below questions:
1. Is there any relationship between age structure of the population and COVID-19 infections?
2. Is there any relationship between gender subgroups and COVID-19 infections?

The project implementation is done using  Map Reduce job in pseudo distributed Hadoop system.

# steps to run the code:

1> Run python3 main.py 
##
2> Copy cleaned input files to hdfs with below command : 
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal input_gender_file.txt hdfs_input_data_path/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal input_age_file.txt hdfs_input_data_path
##
3> Run mapper reducer commands with below commands:
python3 mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs_input_data_path/input_age_file.txt --output hdfs_output_data_path/map_age_reduce_output/  

python3 mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs_input_data_path/input_gender_file.txt --output hdfs_output_data_path/map_gender_reduce_output/
