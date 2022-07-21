"""relate contests and results

Revision ID: 0edf7552fa20
Revises: 6b137a0fac0a
Create Date: 2021-10-20 11:04:51.899419

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0edf7552fa20'
down_revision = '6b137a0fac0a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('results', 'contest_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=False)
    op.create_foreign_key(None, 'results', 'contests', ['contest_id'], ['contest_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'results', type_='foreignkey')
    op.alter_column('results', 'contest_id',
               existing_type=sa.VARCHAR(length=255),
               nullable=True)
    # ### end Alembic commands ###
