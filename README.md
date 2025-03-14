# **Nerd Facts DBT - WIP** 🚀  

This repository is a **work in progress** for an **ETL + DBT + Metabase** setup to fetch, transform, and analyze data from multiple public APIs (PokéAPI, SWAPI, and NetrunnerDB). It extracts data, loads it into **PostgreSQL**, and models it using **dbt** before visualizing it in **Metabase**.  

⚠️ **This project is not yet complete. Expect frequent changes!**  

## **Prerequisites**  

Before cloning and running this project, ensure you have:  
- **Ubuntu** (or any Linux-based system)  
- **Python 3.10+** and `venv`  
- **PostgreSQL** (running on `localhost`)  
- **Node.js 18+** and `npm`  
- **dbt-core** and `dbt-postgres` installed  
- **Metabase** set up for visualization  

---

## **Installation & Setup**  

### **1. Clone the Repository**  
```sh
git clone https://github.com/YOUR_USERNAME/nerd-facts-dbt.git
cd nerd-facts-dbt
2. Set Up Python Virtual Environment
sh
Kopiera
Redigera
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
3. Configure PostgreSQL
Make sure PostgreSQL is running and create the required database:

sh
Kopiera
Redigera
sudo -u postgres psql
sql
Kopiera
Redigera
CREATE DATABASE nerd_facts;
CREATE USER dbt_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE nerd_facts TO dbt_user;
4. Set Up DBT Profile
Ensure your ~/.dbt/profiles.yml is configured like this:

yaml
Kopiera
Redigera
nerd_facts_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: dbt_user
      password: your_secure_password
      port: 5432
      dbname: nerd_facts
      schema: public
5. Install Node.js Dependencies
sh
Kopiera
Redigera
npm install
6. Fetch and Load Data into PostgreSQL
sh
Kopiera
Redigera
python etl/load_data.py
7. Run DBT Models
sh
Kopiera
Redigera
dbt run
8. Set Up and Run Metabase
Start Metabase to visualize the data:

sh
Kopiera
Redigera
java -jar metabase.jar
Access it in a web browser at http://localhost:3000 and connect it to the nerd_facts database.