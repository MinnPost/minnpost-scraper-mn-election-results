"""add elections model.

Revision ID: f8c7a2615780
Revises: 3c82f91f67f5
Create Date: 2022-08-15 09:33:03.391397

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f8c7a2615780'
down_revision = '3c82f91f67f5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('elections',
    sa.Column('id', sa.String(length=255), autoincrement=False, nullable=False),
    sa.Column('base_url', sa.String(length=255), nullable=True),
    sa.Column('election_date', sa.String(length=255), nullable=True),
    sa.Column('contest_count', sa.BigInteger(), nullable=True),
    sa.Column('primary', sa.Boolean(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('message',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('text', sa.Unicode(length=200), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('message')
    op.drop_table('elections')
    # ### end Alembic commands ###
