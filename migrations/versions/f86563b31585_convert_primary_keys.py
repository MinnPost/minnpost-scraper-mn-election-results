"""convert primary keys

Revision ID: f86563b31585
Revises: ff098ed171bc
Create Date: 2022-09-23 20:23:28.354080

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f86563b31585'
down_revision = 'ff098ed171bc'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE areas DROP CONSTRAINT area_id_pkey CASCADE;')
    op.execute('ALTER TABLE contests DROP CONSTRAINT contest_id_pkey CASCADE;')
    op.execute('ALTER TABLE questions DROP CONSTRAINT question_id_pkey CASCADE;')
    op.execute('ALTER TABLE results DROP CONSTRAINT result_id_pkey CASCADE;')


def downgrade():
    op.execute("ALTER TABLE areas ADD CONSTRAINT area_id_pkey PRIMARY KEY (area_id);")
    op.execute("ALTER TABLE contests ADD CONSTRAINT contest_id_pkey PRIMARY KEY (contest_id);")
    op.execute("ALTER TABLE questions ADD CONSTRAINT question_id_pkey PRIMARY KEY (question_id);")
    op.execute("ALTER TABLE results ADD CONSTRAINT result_id_pkey PRIMARY KEY (result_id);")
