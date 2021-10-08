"""create results table

Revision ID: d601baf15e00
Revises: 4b16f3fde538
Create Date: 2021-10-01 12:11:28.961345

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd601baf15e00'
down_revision = '4b16f3fde538'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('questions',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('question_id', sa.Integer(), nullable=False),
    sa.Column('contest_id', sa.String(length=255), nullable=True),
    sa.Column('title', sa.String(length=255), nullable=True),
    sa.Column('sub_title', sa.String(length=255), nullable=True),
    sa.Column('question_body', sa.Text(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('question_id')
    )
    op.create_table('results',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('result_id', sa.String(length=255), nullable=False),
    sa.Column('contest_id', sa.String(length=255), nullable=True),
    sa.Column('office_name', sa.String(length=255), nullable=True),
    sa.Column('candidate_id', sa.String(length=255), nullable=True),
    sa.Column('candidate', sa.String(length=255), nullable=True),
    sa.Column('suffix', sa.String(length=255), nullable=True),
    sa.Column('incumbent_code', sa.String(length=255), nullable=True),
    sa.Column('party_id', sa.String(length=255), nullable=True),
    sa.Column('votes_candidate', sa.BigInteger(), nullable=True),
    sa.Column('percentage', sa.Float(precision=2), nullable=True),
    sa.Column('ranked_choice_place', sa.BigInteger(), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('result_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('results')
    op.drop_table('questions')
    # ### end Alembic commands ###