"""bigger boundary field

Revision ID: 551f9c39a677
Revises: f86563b31585
Create Date: 2022-09-23 21:36:30.691230

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '551f9c39a677'
down_revision = 'f86563b31585'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('contests', 'boundary',
               existing_type=sa.VARCHAR(length=255),
               type_=sa.String(length=510),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('contests', 'boundary',
               existing_type=sa.String(length=510),
               type_=sa.VARCHAR(length=255),
               existing_nullable=True)
    # ### end Alembic commands ###
