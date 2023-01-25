import json
from typing import List

from mev_inspect.models.total_profits import TotalProfitsModel
from mev_inspect.schemas.total_profits import TotalProfits


def write_total_profits_for_blocks(
    inspect_db_session,
    total_profits_for_blocks: List[TotalProfits],
) -> None:
    models = [
        TotalProfitsModel(**json.loads(swap.json()))
        for swap in total_profits_for_blocks
    ]

    inspect_db_session.bulk_save_objects(models)
    inspect_db_session.commit()


def delete_existing_blocks(
    inspect_db_session,
    from_block,
    to_block,
) -> None:
    inspect_db_session.execute(
        f"DELETE FROM total_profit_by_block WHERE block_number >= {from_block} AND block_number <= {to_block}"
    )
