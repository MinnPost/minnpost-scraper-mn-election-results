"""change the other primary key fields

Revision ID: c357ef4f2d5c
Revises: 535e592d44fa
Create Date: 2021-10-07 20:50:19.796355

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c357ef4f2d5c'
down_revision = '535e592d44fa'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('contests_contest_id_key', 'contests', type_='unique')
    op.drop_column('contests', 'id')
    op.drop_constraint('meta_key_key', 'meta', type_='unique')
    op.drop_column('meta', 'id')
    op.drop_constraint('questions_question_id_key', 'questions', type_='unique')
    op.drop_column('questions', 'id')
    op.drop_constraint('results_result_id_key', 'results', type_='unique')
    op.drop_column('results', 'id')
    op.execute("ALTER TABLE contests ADD CONSTRAINT contest_id_pkey PRIMARY KEY (contest_id);")
    op.execute("ALTER TABLE meta ADD CONSTRAINT meta_key_pkey PRIMARY KEY (key);")
    op.execute("ALTER TABLE questions ADD CONSTRAINT question_id_pkey PRIMARY KEY (question_id);")
    op.execute("ALTER TABLE results ADD CONSTRAINT result_id_pkey PRIMARY KEY (result_id);")
    
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('results', sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False))
    op.create_unique_constraint('results_result_id_key', 'results', ['result_id'])
    op.add_column('questions', sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False))
    op.create_unique_constraint('questions_question_id_key', 'questions', ['question_id'])
    op.add_column('meta', sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False))
    op.create_unique_constraint('meta_key_key', 'meta', ['key'])
    op.add_column('contests', sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False))
    op.create_unique_constraint('contests_contest_id_key', 'contests', ['contest_id'])
    op.execute("ALTER TABLE contests DROP CONSTRAINT contest_id_pkey;")
    op.execute("ALTER TABLE meta DROP CONSTRAINT meta_key_pkey;")
    op.execute("ALTER TABLE questions DROP CONSTRAINT question_id_pkey;")
    op.execute("ALTER TABLE questions DROP CONSTRAINT result_id_pkey;")
    # ### end Alembic commands ###
