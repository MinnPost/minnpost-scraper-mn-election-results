"""try to fix ids

Revision ID: ff098ed171bc
Revises: fbfcb399f0c5
Create Date: 2022-09-23 20:16:08.697408

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ff098ed171bc'
down_revision = 'fbfcb399f0c5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('areas', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=False,
               autoincrement=False)
    op.alter_column('contests', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=False,
               autoincrement=False)
    op.alter_column('questions', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=False)
    op.alter_column('results', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=False,
               autoincrement=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('results', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=True,
               autoincrement=False)
    op.alter_column('questions', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=True)
    op.alter_column('contests', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=True,
               autoincrement=False)
    op.alter_column('areas', 'election_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=True,
               autoincrement=False)
    # ### end Alembic commands ###