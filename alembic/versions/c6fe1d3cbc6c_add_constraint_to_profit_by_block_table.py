"""add constraint to profit by block table

Revision ID: c6fe1d3cbc6c
Revises: a5a44a7c854d
Create Date: 2023-01-24 17:15:41.319990

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "c6fe1d3cbc6c"
down_revision = "a5a44a7c854d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint(
        "uq_total_profit_by_block_transaction_hash",
        "total_profit_by_block",
        ["transaction_hash"],
    )


def downgrade():
    op.drop_constraint(
        "uq_total_profit_by_block_transaction_hash", "total_profit_by_block"
    )
