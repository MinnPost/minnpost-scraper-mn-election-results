"""change primary key field

Revision ID: 535e592d44fa
Revises: ee3fd7a8da82
Create Date: 2021-10-07 20:26:17.001898

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '535e592d44fa'
down_revision = 'ee3fd7a8da82'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE areas ADD CONSTRAINT area_id_pkey PRIMARY KEY (area_id);")
    # ### end Alembic commands ###


def downgrade():
    op.execute("ALTER TABLE areas DROP CONSTRAINT area_id_pkey;")
    # ### end Alembic commands ###
