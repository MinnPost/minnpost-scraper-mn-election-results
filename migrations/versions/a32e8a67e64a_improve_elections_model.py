"""improve elections model.

Revision ID: a32e8a67e64a
Revises: f8c7a2615780
Create Date: 2022-08-17 08:08:11.920525

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a32e8a67e64a'
down_revision = 'f8c7a2615780'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('elections', 'contest_count')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('elections', sa.Column('contest_count', sa.BIGINT(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
