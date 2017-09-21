These files contain the source code for the project posted at

http://contributions-interest.herokuapp.com

The purpose of this project is to investigate the relationship between contributions made to nonprofit organizations and the Google search volume for those organizations. The project is described in detail on the project website posted above.

The data used for this project includes the following:

Financial data obtained from the IRS form 990 database for the years 2011,2012,2013, and 2014. Specifically, we obtain from this database a list of employer identification numbers (EINs), each corresponding to a nonprofit organization, and the total contributions for each of the years 2011,2012,2013, and 2014 for that organization (link: https://www.irs.gov/statistics/soi-tax-stats-annual-extract-of-tax-exempt-organization-financial-data).

A list of organization names corresponding to the EINs in the form 990 database (since the form 990 database lists organizations just by EIN rather than including organization names), obtained from ProPublica (link: https://www.propublica.org/datastore/api/nonprofit-explorer-api).

Google search volume data, obtained from Google Trends using the pytrends library (link: https://github.com/GeneralMills/pytrends).

Here is an overview of the usage of these files:

Run load_irs_data.py, load_irs_data_eins_contributions, generate_ein_lists_for_name_collection.py, and read_irs_database, in that order, to generate a database in which each row contains the EIN and name of an organization along with the total contributions to that organization for each of the years 2011, 2012, 2013, and 2014. To append to this database the search volume data from Google trends, run anchor_trends_data.py, trends_reader.py, and irs_trends_database_builder.py, in that order. Finally, run irs_trends_visualize.py to generate the visualization shown at contributions-interest.herokuapp.com.
