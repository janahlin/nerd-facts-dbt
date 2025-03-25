# **Nerd Facts DBT - WIP** 🚀  

This repository is a **work in progress** for an **ETL + DBT + Evidence.dev** setup to fetch, transform, and analyze data from multiple public APIs (PokéAPI, SWAPI, and NetrunnerDB). It extracts data, loads it into **PostgreSQL**, and models it using **dbt** before visualizing it in **Evidence.dev**.  

⚠️ **This project is not yet complete. Expect frequent changes!**  

## **Prerequisites**  

Before cloning and running this project, ensure you have:  
- **Ubuntu** (or any Linux-based system)  
- **Python 3.10+** and `venv`  
- **PostgreSQL** (running on `localhost`)  
- **Node.js 18+** and `npm`  
- **dbt-core** and `dbt-postgres` installed  
- **Evidence.dev** for visualization  

---

## **Installation & Setup**  

### **1. Clone the Repository**  
```sh
git clone https://github.com/YOUR_USERNAME/nerd-facts-dbt.git
cd nerd-facts-dbt
```

### **2. Set Up Python Virtual Environment**
```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### **3. Configure PostgreSQL**
Make sure PostgreSQL is running and create the required database:
```sh
sudo -u postgres psql
```
```sql
CREATE DATABASE nerd_facts;
CREATE USER dbt_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE nerd_facts TO dbt_user;
```

### **4. Set Up DBT Profile**
Ensure your ~/.dbt/profiles.yml is configured like this:
```yaml
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
```

### **5. Fetch and Load Data into PostgreSQL**
```sh
python etl/load_data.py
```

### **6. Run DBT Models**
```sh
cd dbt_project
dbt run
```

### **7. Set Up and Run Evidence.dev**
Start Evidence.dev to visualize the data:
```sh
cd dbt_project/reports
npm install
npm run dev
```
Access it in a web browser at http://localhost:4000

## **Current Status & Next Steps**
✅ ETL pipeline fetches data from APIs  
✅ Data loads into PostgreSQL  
✅ DBT models transform and store in public schema  
✅ Evidence.dev connects for visualization  
🔄 Next Steps: Improve data models, add more analytics, and automate updates  

## **License**
📜 MIT License – Free to use and modify.