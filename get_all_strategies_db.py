from ctrader_profiles_data import profiles_dict
from get_strategy_positions import get_strategy_opened_positions
import sqlalchemy

def all_strategies_pos(db_engine):
    for pr in profiles_dict.keys():
        id = profiles_dict[pr]["profileId"]

        pos = get_strategy_opened_positions(pr)
        if not pos.empty:
            pos.to_sql(f'{id}', db_engine,
                          if_exists='replace')
