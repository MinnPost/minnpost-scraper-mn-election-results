"""Initial migration.

Revision ID: 79f24a06e40e
Revises: 
Create Date: 2021-10-01 10:44:19.216142

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '79f24a06e40e'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('areas',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('area_id', sa.String(length=255), nullable=False),
    sa.Column('areas_group', sa.String(length=255), nullable=True),
    sa.Column('county_id', sa.String(length=255), nullable=True),
    sa.Column('county_name', sa.String(length=255), nullable=True),
    sa.Column('ward_id', sa.String(length=255), nullable=True),
    sa.Column('precinct_id', sa.String(length=255), nullable=True),
    sa.Column('precinct_name', sa.String(length=255), nullable=True),
    sa.Column('state_senate_id', sa.String(length=255), nullable=True),
    sa.Column('state_house_id', sa.String(length=255), nullable=True),
    sa.Column('county_commissioner_id', sa.String(length=255), nullable=True),
    sa.Column('district_court_id', sa.String(length=255), nullable=True),
    sa.Column('soil_water_id', sa.String(length=255), nullable=True),
    sa.Column('school_district_id', sa.String(length=255), nullable=True),
    sa.Column('school_district_name', sa.String(length=255), nullable=True),
    sa.Column('mcd_id', sa.String(length=255), nullable=True),
    sa.Column('precincts', sa.String(length=255), nullable=True),
    sa.Column('name', sa.String(length=255), nullable=True),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('area_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('areas')
    # ### end Alembic commands ###
