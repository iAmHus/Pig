log_data = load '../exampleDatasets/log_data.txt' using PigStorage(' ');
log_data_1 = filter log_data by not $3 == 'message';
log_data_2= foreach log_data_1 generate SUBSTRING($0,1,11),$1,SUBSTRING($2,0,8),$3,$4,$5,$6,$7,$8,$9,$10;
log_data_3 = foreach log_data_2 generate CONCAT($0,' '),$2,$3,$4,$5,$6,$7,$8,$9,$10;
log_data_4 = foreach log_data_3 generate CONCAT($0,$1),$2,$3,$4,$5,$6,$7,$8,$9;
log_data_5 = foreach log_data_4 generate ToDate($0,'yyyy-mm-dd HH:mm:ss'),$1,$2,$3,$4,$5,$6,$7,$8;
log_data_6 = foreach log_data_5 generate $0 as Time_Stamp,GetMonth($0) as Month,GetDay($0) as Day,$1 as
server_number,$2 as type_of_message,$3 as cluster_name,$4 as process_ID,$5 as thread_ID ,$6 as module,$7 as error_type
,$8 as error_message;
split log_data_6 into warnings_data if type_of_message == 'warning', error_data if type_of_message == 'Error';
groupd = group error_data by (server_number,error_type);
error_summary = foreach groupd{
								number_of_occurences = COUNT(error_data);
								error_messages = BagToString(error_data.error_message,',');
								error_data = foreach error_data generate server_number, error_type, Time_Stamp, process_ID, thread_ID, module;
								sorted_error_messages = order error_data by Time_Stamp desc;
								top_item = limit sorted_error_messages 1;
								generate flatten(top_item),number_of_occurences,error_messages;
								};
store error_summary into 'error_summary';
store error_data into 'error_data';
store warnings_data into 'warnings_data';
