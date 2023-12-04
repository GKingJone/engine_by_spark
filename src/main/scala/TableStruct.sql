
solrid              	string              	                    
platenumber         	string              	                    
capturetime         	long
directionid         	int                 	                    
colorid             	int                 	                    
modelid             	int                 	                    
brandid             	int                 	                    
levelid             	int                 	                    
yearid              	int                 	                    
recfeature          	string   
locationid          	string
lastcaptured		int
issheltered		int
createTime 		long
regioncode 		int	
direction		string   
dateid              	int                 	                    
             	                    
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
dateid              	int                 	                             	                    
Time taken: 0.099 seconds, Fetched: 18 row(s)

 


CREATE TABLE pass_info (
 solrid STRING,
 platenumber STRING,
 capturetime BIGINT
 directionid INT,
 colorid INT,
 modelid INT,
 brandid INT,
 levelid INT,
 yearid INT,
 recfeature STRING,
 locationId STRING,
) 
PARTITIONED BY (dateid INT)
STORED AS PARQUET;





                