playersCSV = 
	LOAD '/user/maria_dev/diplomacy/players.csv' 
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
	AS
    	(game_id:int,
        country:chararray,
        won:int,
        num_supply_centers:int,
        eliminated:int,
        start_turn:int,
        end_turn:int);

wins = FILTER playersCSV BY won == 1;
grouped_by = GROUP wins BY country;
counts = FOREACH grouped_by GENERATE group, COUNT($1);

DUMP counts;