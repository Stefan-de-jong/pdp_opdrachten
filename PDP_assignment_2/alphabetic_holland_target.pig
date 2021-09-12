ordersCSV = 
	LOAD '/user/maria_dev/diplomacy/orders.csv' 
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
	AS
    	(game_id:int,
        unit_id:int,
        unit_order:chararray,
        location:chararray,
        target:chararray,
        target_dest:chararray,
        success:int,
        reason:chararray,
        turn_num:int);
        
start_locations = FOREACH ordersCSV GENERATE location, target;
targeted_holland = FILTER start_locations BY target == 'Holland';
grouped = GROUP targeted_holland BY location;
counts = FOREACH grouped GENERATE $0, 'Holland', COUNT($1);
order_by = ORDER counts BY $0;

DUMP order_by;