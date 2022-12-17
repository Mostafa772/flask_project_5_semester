from datetime import *
import datetime
from flask import Flask
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from sqlalchemy import Table, Column, create_engine, MetaData, Integer, DateTime, BOOLEAN
import pandas as pd
from flask_restful import Api, Resource, reqparse, abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


def update_dataset():
    """
    It checks if the dataset has been updated downloads it again
    and turns a flag for reloading the database
    :return:
    """
    update_flag = bool
    resp3 = requests.get("https://data.cityofnewyork.us/City-Government/NYC-Jobs/kpav-sd4t")
    update_history_index = 770000 + resp3.text[770000: 780000].find("lastUpdatedAt") + 16
    last_upd = resp3.text[update_history_index: update_history_index + 10]
    old_date = datetime.datetime(int(last_update[:4]), int(last_update[4:6]), int(last_update[6:8]))

    # if (old_date ):
    return True


db = SQLAlchemy()
app = Flask(__name__)
api = Api(app)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
db_string = "postgresql+psycopg2://mostafa:123@postgres/new-york-city-api"
app.config['SQLALCHEMY_DATABASE_URI'] = db_string
en = create_engine(db_string)
# use connect function to establish the connection
# conn = en.connect()
db.init_app(app)
Base = declarative_base()
# en = create_engine("sqlite:///database.db")
dbsession = sessionmaker(bind=en)
session = dbsession()
db.init_app(app)
meta = MetaData()

NY_data = Table(
    'NY_data', meta,
    Column("job_id", Integer, primary_key=True),
    Column("agency", db.String(100), nullable=False),
    Column("posting_type", db.String(100), nullable=False),
    Column("num_of_positions", Integer, nullable=True),
    Column("business_title", db.String(100), nullable=False),
    Column("civil_service_title", db.String(200), nullable=False),
    Column("title_classification", db.String(100), nullable=True),
    Column("title_code_no", db.String(100), nullable=True),
    Column("level", db.String(100), nullable=True),
    Column("job_category", db.String(100), nullable=True),
    Column("full_time_part_time_flag", db.BOOLEAN, nullable=True) , ## True if full time False if part
    Column("Career_level", db.String(100), nullable=True),
    Column("salary_range_from", db.String(100), nullable=True),
    Column("salary_range_to", db.String(100), nullable=True),
    Column("salary_frequency", db.String(100), nullable=True),
    Column("work_location", db.String(100), nullable=True),
    Column("division", db.String(100), nullable=True),
    Column("job_description", db.String(100), nullable=True),
    Column("minimum_qual_requirements", db.String(100), nullable=True),
    Column("preferred_skills", db.String(100), nullable=True),
    Column("additional_information", db.String(100), nullable=True),
    Column("to_apply", db.String(200), nullable=True),
    Column("hours", db.String(100), nullable=True),
    Column("work_location_1", db.String(100), nullable=True),
    Column("recruitment_contact", db.String(100), nullable=True),
    Column("posting_date", DateTime, nullable=True),
    Column("post_until", db.String(100), nullable=True),
    Column("posting_updated", DateTime, nullable=True),
    Column("process_date", DateTime, nullable=True),
    Column("years", Integer, nullable=True),
    Column("avg_annual_salary", db.Float, nullable=False),
)

Base.metadata.create_all(en)


resp2 = requests.get("https://data.cityofnewyork.us/resource/kpav-sd4t.json?$limit=50000")
data = resp2.text
df = pd.read_json(data)

dates = []
for x in range(df.shape[0]):
    year = int(df["posting_date"][x][:4])
    month = int(df["posting_date"][x][5:7])  # 1
    day = int(df["posting_date"][x][8:10])
    d = datetime.date(year, month, day).strftime("%Y-%m-%d")
    dates.append(d)

years = []
for x in range(df.shape[0]):
    year = int(df["posting_date"][x][:4])
    years.append(year)
print(set(years))
df['years'] = years
#
number_of_weeks = 52
number_of_days = 5
number_of_hours = 8
avg_annual_salary = []
print(type(df.shape[0]))
for x in range(df.shape[0]):
    if df['salary_frequency'][x] == 'Annual':
        avg_annual_salary.append((df['salary_range_from'][x] + df['salary_range_to'][x]) / 2)
    elif df['salary_frequency'][x] == 'Daily':
        avg_annual_salary.append(
            ((df['salary_range_from'][x] + df['salary_range_to'][
                x]) / 2) * number_of_days * number_of_weeks)
    elif df['salary_frequency'][x] == 'Hourly':
        avg_annual_salary.append(((df['salary_range_from'][x] + df['salary_range_to'][
            x]) / 2) * number_of_hours * number_of_days * number_of_weeks)

df['avg_annual_salary'] = avg_annual_salary

print("What is the happening: ", df.to_sql('ny_data', en, if_exists='append'))

user = Table(
    'user', meta,
    Column('id', Integer, primary_key=True),
    Column('email', db.String(100), nullable=False),
    Column('password', Integer, nullable=False),
)

last_update = Table(
    'last_update', meta,
    Column('id', Integer, primary_key=True),
    Column('date', DateTime, nullable=False),
)

Base.metadata.create_all(en)


def num_of_jobs_in_year_graph(x: str):
    """
    it receives x which is the title of the occupation
    and returns the graph that represents the number of
    open jobs for that occupation every year
    :param x: string title of the occupation
    :return: void
    """
    # print(x)
    df_date = pd.read_sql(
        'SELECT years, civil_service_title, Count(civil_service_title) as num_of_jobs FROM ny_data group by'
        ' years, civil_service_title', en)
    tt = df_date.where(df_date.civil_service_title == x)
    tt.dropna()
    sns.barplot(x="years", y="num_of_jobs", hue='civil_service_title', data=tt)
    plt.xticks(rotation=45)
    plt.title(x)
    plt.legend([], [], frameon=False)
    plt.show()


num_of_jobs_in_year_graph("CONTRACT SPECIALIST")


def the_most_paying_jobs(x: int):
    """
    it shows the x most paid jobs in the dataset
    :param x: the number of rows to show from the header function
    :return:
    """
    df_salary = pd.read_sql(
        'SELECT civil_service_title, avg_annual_salary FROM ny_data', en)

    occupation_mean_salary = pd.DataFrame(
        df_salary.groupby(['civil_service_title'])['avg_annual_salary'].mean()).reset_index()

    sort = occupation_mean_salary.sort_values('avg_annual_salary', ascending=False)
    # print(sort.head(x))
    return sort.head(x).to_json()


the_most_paying_jobs(2)


def job_avg_salary(x: str):
    """
    it shows the avg salary of the job x
    :param x: string the civil service title of the job
    :return:
    """
    df_salary = pd.read_sql(
        'SELECT civil_service_title, avg_annual_salary FROM ny_data', en)

    occupation_mean_salary = pd.DataFrame(
        df_salary.groupby(['civil_service_title'])['avg_annual_salary'].mean()).reset_index()
    # print(occupation_mean_salary.head())
    # print(occupation_mean_salary[occupation_mean_salary['civil_service_title'] == "CONTRACT SPECIALIST"])
    return occupation_mean_salary[occupation_mean_salary['civil_service_title'] == x].to_json()


job_avg_salary("CONTRACT SPECIALIST")


class AverageSalary(Resource):
    def get(self, job_title):
        return job_avg_salary(job_title)


class MostPayingJobs(Resource):
    def get(self, num_of_rows):
        return the_most_paying_jobs(num_of_rows)


api.add_resource(MostPayingJobs, "/most_paying_jobs/<int:num_of_rows>")
api.add_resource(AverageSalary, "/avg_salary/<string:job_title>")
# result = session.query(NY_data.years)
# print("Query 1: ", result)

# for r in result:
#     print(r[0])


if __name__ == '__main__':
    app.run(debug=True)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

# print(db.select([NY_data.agency]))
# select_all = session.query(NY_data).all()
# select_all.update(u, )
# dates = []
# df_date = pd.read_sql('SELECT posting_date FROM NY_data', en)
# for x in range(df_date.shape[0]):
#     year = int(df_date["posting_date"][x][:4])
#     month = int(df_date["posting_date"][x][5:7])  # 1
#     day = int(df_date["posting_date"][x][8:10])
#     d = datetime.date(year, month, day).strftime("%Y-%m-%d")
#     dates.append(d)
# # print(dates)
#
# years_2 = []
# for x in range(df_date.shape[0]):
#     year = int(df_date["posting_date"][x][:4])
#     years_2.append(year)
#
# print(set(years_2))

# LastUpdate(Base):
#     __tablename__ = 'last_update'
#     id = db.Column(db.Integer, primary_key=True)
#     date = db.Column(db.DateTime, nullable=False)


# count = 0
# for x in df_date["civil_service_title"]:
#     if count >= 5:
#         break
#     tt = df_date.where(df_date.civil_service_title == x)
#     # print(tt)
#     tt.dropna()
#
#     sns.barplot(x="years", y="num_of_jobs", hue='civil_service_title', data=tt)
#     plt.xticks(rotation=45)
#     plt.title(x)
#     plt.legend([], [], frameon=False)
#     plt.show()
#     count += 1

# df_date = pd.read_sql(
#     'SELECT years, civil_service_title, Count(civil_service_title) as num_of_jobs FROM NY_data group by'
#     ' years, civil_service_title', en)
# print(df_date)
# get the plots of the jobs in each year
# GET THE 20 MOST PAYING JOBS


# df_salary = pd.read_sql(
#     'SELECT civil_service_title, avg_annual_salary FROM NY_data', en)
#
# occupation_mean_salary = pd.DataFrame(
#     df_salary.groupby(['civil_service_title'])['avg_annual_salary'].mean()).reset_index()
#
# sort = occupation_mean_salary.sort_values('avg_annual_salary', ascending=False)


"""
Notes from the class 28/11/2022



"""


# class NY_data(db.Model):
#
#     job_id = db.Column(db.Integer, primary_key=True)
#     agency = db.Column(db.String(100), nullable=False)
#     posting_type = db.Column(db.String(100), nullable=False)
#     num_of_positions = db.Column(db.Integer, nullable=True)
#     business_title = db.Column(db.String(100), nullable=False)
#     civil_service_title = db.Column(db.String(200), nullable=False)
#     title_classification = db.Column(db.String(100), nullable=True)
#     title_code_no = db.Column(db.String(100), nullable=True)
#     level = db.Column(db.String(100), nullable=True)
#     job_category = db.Column(db.String(100), nullable=True)
#     full_time_part_time_flag = db.Column(db.BOOLEAN, nullable=True)  ## True if full time False if part
#     career_level = db.Column(db.String(100), nullable=True)
#     salary_range_from = db.Column(db.String(100), nullable=True)
#     salary_range_to = db.Column(db.String(100), nullable=True)
#     salary_frequency = db.Column(db.String(100), nullable=True)
#     work_location = db.Column(db.String(100), nullable=True)
#     division = db.Column(db.String(100), nullable=True)
#     job_description = db.Column(db.String(100), nullable=True)
#     minimum_qual_requirements = db.Column(db.String(100), nullable=True)
#     preferred_skills = db.Column(db.String(100), nullable=True)
#     additional_information = db.Column(db.String(100), nullable=True)
#     to_apply = db.Column(db.String(200), nullable=True)
#     hours = db.Column(db.String(100), nullable=True)
#     work_location_1 = db.Column(db.String(100), nullable=True)
#     recruitment_contact = db.Column(db.String(100), nullable=True)
#     posting_date = db.Column(db.DateTime, nullable=True)
#     post_until = db.Column(db.String(100), nullable=True)
#     posting_updated = db.Column(db.DateTime, nullable=True)
#     process_date = db.Column(db.DateTime, nullable=True)
#     years = db.Column(db.Integer, nullable=True)
#     avg_annual_salary = db.Column(db.Float, nullable=False)


# class NY_data(db.Model):
#
#     job_id = db.Column(db.Integer, primary_key=True)
#     agency = db.Column(db.String(100), nullable=False)
#     posting_type = db.Column(db.String(100), nullable=False)
#     num_of_positions = db.Column(db.Integer, nullable=True)
#     business_title = db.Column(db.String(100), nullable=False)
#     civil_service_title = db.Column(db.String(200), nullable=False)
#     title_classification = db.Column(db.String(100), nullable=True)
#     title_code_no = db.Column(db.String(100), nullable=True)
#     level = db.Column(db.String(100), nullable=True)
#     job_category = db.Column(db.String(100), nullable=True)
#     full_time_part_time_flag = db.Column(db.BOOLEAN, nullable=True)  ## True if full time False if part
#     career_level = db.Column(db.String(100), nullable=True)
#     salary_range_from = db.Column(db.String(100), nullable=True)
#     salary_range_to = db.Column(db.String(100), nullable=True)
#     salary_frequency = db.Column(db.String(100), nullable=True)
#     work_location = db.Column(db.String(100), nullable=True)
#     division = db.Column(db.String(100), nullable=True)
#     job_description = db.Column(db.String(100), nullable=True)
#     minimum_qual_requirements = db.Column(db.String(100), nullable=True)
#     preferred_skills = db.Column(db.String(100), nullable=True)
#     additional_information = db.Column(db.String(100), nullable=True)
#     to_apply = db.Column(db.String(200), nullable=True)
#     hours = db.Column(db.String(100), nullable=True)
#     work_location_1 = db.Column(db.String(100), nullable=True)
#     recruitment_contact = db.Column(db.String(100), nullable=True)
#     posting_date = db.Column(db.DateTime, nullable=True)
#     post_until = db.Column(db.String(100), nullable=True)
#     posting_updated = db.Column(db.DateTime, nullable=True)
#     process_date = db.Column(db.DateTime, nullable=True)
#     years = db.Column(db.Integer, nullable=True)
#     avg_annual_salary = db.Column(db.Float, nullable=False)
