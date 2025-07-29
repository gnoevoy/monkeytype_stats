-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results_incremental` as DBT_INTERNAL_DEST
        using (

select *
from `global-grammar-449122-b6`.`monkeytype_stats`.`results`


    where timestamp > (select max(timestamp) from `global-grammar-449122-b6`.`monkeytype_stats_analytics`.`results_incremental`)

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE._id = DBT_INTERNAL_DEST._id))

    
    when matched then update set
        `_id` = DBT_INTERNAL_SOURCE.`_id`,`wpm` = DBT_INTERNAL_SOURCE.`wpm`,`acc` = DBT_INTERNAL_SOURCE.`acc`,`rawWpm` = DBT_INTERNAL_SOURCE.`rawWpm`,`consistency` = DBT_INTERNAL_SOURCE.`consistency`,`mode` = DBT_INTERNAL_SOURCE.`mode`,`mode2` = DBT_INTERNAL_SOURCE.`mode2`,`restartCount` = DBT_INTERNAL_SOURCE.`restartCount`,`testDuration` = DBT_INTERNAL_SOURCE.`testDuration`,`afkDuration` = DBT_INTERNAL_SOURCE.`afkDuration`,`incompleteTestSeconds` = DBT_INTERNAL_SOURCE.`incompleteTestSeconds`,`punctuation` = DBT_INTERNAL_SOURCE.`punctuation`,`numbers` = DBT_INTERNAL_SOURCE.`numbers`,`language` = DBT_INTERNAL_SOURCE.`language`,`timestamp` = DBT_INTERNAL_SOURCE.`timestamp`,`correctChars` = DBT_INTERNAL_SOURCE.`correctChars`,`incorrectChars` = DBT_INTERNAL_SOURCE.`incorrectChars`,`extraChars` = DBT_INTERNAL_SOURCE.`extraChars`,`missedChars` = DBT_INTERNAL_SOURCE.`missedChars`
    

    when not matched then insert
        (`_id`, `wpm`, `acc`, `rawWpm`, `consistency`, `mode`, `mode2`, `restartCount`, `testDuration`, `afkDuration`, `incompleteTestSeconds`, `punctuation`, `numbers`, `language`, `timestamp`, `correctChars`, `incorrectChars`, `extraChars`, `missedChars`)
    values
        (`_id`, `wpm`, `acc`, `rawWpm`, `consistency`, `mode`, `mode2`, `restartCount`, `testDuration`, `afkDuration`, `incompleteTestSeconds`, `punctuation`, `numbers`, `language`, `timestamp`, `correctChars`, `incorrectChars`, `extraChars`, `missedChars`)


    