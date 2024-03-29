"""use id field

Revision ID: 3c82f91f67f5
Revises: 0edf7552fa20
Create Date: 2021-10-25 09:38:23.650503

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3c82f91f67f5'
down_revision = '0edf7552fa20'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('areas', 'area_id', nullable=False, new_column_name='id')
    op.alter_column('contests', 'contest_id', nullable=False, new_column_name='id')
    op.alter_column('questions', 'question_id', nullable=False, new_column_name='id')
    op.alter_column('results', 'result_id', nullable=False, new_column_name='id')
    
    op.drop_constraint('results_contest_id_fkey', 'results', type_='foreignkey')
    op.create_foreign_key(None, 'results', 'contests', ['contest_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('results', sa.Column('result_id', sa.VARCHAR(length=255), autoincrement=False, nullable=False))
    op.drop_constraint(None, 'results', type_='foreignkey')
    op.create_foreign_key('results_contest_id_fkey', 'results', 'contests', ['contest_id'], ['contest_id'])
    op.drop_column('results', 'id')
    op.add_column('questions', sa.Column('question_id', sa.VARCHAR(length=255), autoincrement=False, nullable=False))
    op.drop_column('questions', 'id')
    op.add_column('contests', sa.Column('contest_id', sa.VARCHAR(length=255), autoincrement=False, nullable=False))
    op.drop_column('contests', 'id')
    op.add_column('areas', sa.Column('area_id', sa.VARCHAR(length=255), autoincrement=False, nullable=False))
    op.drop_column('areas', 'id')
    # ### end Alembic commands ###
