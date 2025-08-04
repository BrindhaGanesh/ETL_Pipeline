# 🌍 Carbon Intensity ELT Pipeline (Ember API + Airflow + PostgreSQL)

This project is a simple but complete **ELT pipeline** that extracts **carbon intensity data** from [Ember's Energy API](https://ember-climate.org/), transforms it for use in machine learning, and loads it into a **PostgreSQL database**. The whole setup is automated using **Apache Airflow** and deployed locally with **Docker Compose**.

---

## 🔧 Tools & Technologies

- **Python**: for writing extract, transform, and load logic
- **PostgreSQL**: as the database backend
- **Docker + Docker Compose**: to isolate and run the environment
- **Apache Airflow**: for scheduling and managing ETL jobs
- **Ember API**: public data source for carbon intensity

---

## 📦 What the Pipeline Does

1. **Extract**: Calls Ember's API and pulls carbon intensity data for all countries since 2000
2. **Transform**: Cleans and formats the data for machine learning use
3. **Load**: Saves the structured data into a PostgreSQL table
4. **Orchestrate**: Airflow runs this automatically (e.g. weekly), handles failures, and logs everything

## Project Goals
1. Build a working ELT pipeline from scratch
2. Automate data integration and cleanup using real-world APIs
3. Prepare the data for future machine learning tasks
 
 
