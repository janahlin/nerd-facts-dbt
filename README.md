# **Nerd Facts DBT** 🚀  

This repository is an **ETL + DBT + Evidence.dev** setup to fetch, transform, and analyze data from multiple public APIs (PokéAPI, SWAPI, and NetrunnerDB). It extracts data, loads it into **PostgreSQL**, and models it using **dbt** before visualizing it in **Evidence.dev**.  

## **Project Structure**

### **Data Models**

The project uses a modern data modeling approach with:

1. **Source Layer**: Raw data from APIs loaded into PostgreSQL
2. **Staging Layer**: Cleaned and standardized data in schemas
3. **Marts Layer**: Subject-oriented fact and dimension tables
4. **OBT Layer**: One Big Table (OBT) models for simplified analytics:
   - `star_wars_obt`: Denormalized Star Wars characters, planets, and films
   - `pokemon_obt`: Denormalized Pokémon species, types, and abilities
   - `nerd_universe_obt`: Cross-universe OBT for unified entity analysis

### **Dashboards**

Evidence.dev dashboards available at http://localhost:3000:

1. **Home Dashboard**: Overview and navigation
2. **Star Wars Dashboard**: Character, planet, and film analysis
3. **Pokémon Dashboard**: Pokémon stats, types, and abilities
4. **OBT Analysis Dashboard**: Cross-universe comparisons and OBT pattern showcase

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
git clone https://github.com/janahlin/nerd-facts-dbt.git
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
Access it in a web browser at http://localhost:3000

## **Features & Highlights**

- **Multi-universe Data Integration**: Combines data from Star Wars, Pokémon, and Netrunner universes
- **One Big Table (OBT) Pattern**: Demonstrates denormalized models for analytics
- **Cross-Universe Analysis**: Compare entities across different fictional universes
- **Interactive Dashboards**: Rich visualizations with Evidence.dev
- **Modern Data Stack**: Complete ETL + Transformation + Visualization pipeline

## **OBT Model Pattern**

The One Big Table (OBT) pattern offers several key benefits:

1. **Simplified Queries**: No complex joins required for analysis
2. **Improved Performance**: Pre-joined data leads to faster queries
3. **Standardized Metrics**: Common attributes across domains
4. **Cross-Domain Analysis**: Compare entities from different universes
5. **Reduced Complexity**: Simplified data model for reporting 

## **Coming Improvements**

1. **More data from API**: Getting detailed data from Pokeapi and NetrunnerDB
2. **Optimize data models**: The data models are rudimentary get a prototype out, need work to work better
3. **Visualize more data**: Not all data is visualized