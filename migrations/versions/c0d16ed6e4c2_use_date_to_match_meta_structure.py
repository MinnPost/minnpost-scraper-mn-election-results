"""use date to match meta structure

Revision ID: c0d16ed6e4c2
Revises: 0ffb83856183
Create Date: 2022-08-17 11:09:01.395902

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c0d16ed6e4c2'
down_revision = '0ffb83856183'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('elections', sa.Column('date', sa.String(length=255), nullable=True))
    op.drop_column('elections', 'election_date')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('elections', sa.Column('election_date', sa.VARCHAR(length=255), autoincrement=False, nullable=True))
    op.drop_column('elections', 'date')
    # ### end Alembic commands ###