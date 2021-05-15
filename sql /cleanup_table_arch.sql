create or replace procedure nba.cleanup_table_arch()
language plpgsql
as $BODY$
begin
    for i in select * from information_schema.columns where table_name = 'player_per_game'
    loop
        
    end loop;
end;$BODY$
/