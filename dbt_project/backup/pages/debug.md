# Database Debug

## Available Tables

```sql available_tables
SELECT 
  table_schema, 
  table_name 
FROM 
  information_schema.tables
WHERE 
  table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY 
  table_schema, table_name
```

<DataTable 
  data={available_tables} 
  search=true 
/>

## Available Schemas

```sql available_schemas
SELECT 
  schema_name
FROM 
  information_schema.schemata
ORDER BY 
  schema_name
```

<DataTable 
  data={available_schemas} 
/>

## Database Information

```sql database_info
SELECT 
  current_database() as current_db,
  current_user as current_user,
  (SELECT count(*) FROM information_schema.tables 
   WHERE table_schema NOT IN ('pg_catalog', 'information_schema')) as total_tables;
```

<DataTable 
  data={database_info} 
/>

## PostgreSQL Connection Test

```sql postgres_test
SELECT 
  current_database() as database,
  current_user as user,
  (SELECT count(*) FROM information_schema.tables 
   WHERE table_schema NOT IN ('pg_catalog', 'information_schema')) as total_tables;
```

<DataTable data={postgres_test} />

```sql direct_test
SELECT 
  current_database() as database,
  current_user as user,
  (SELECT count(*) FROM information_schema.tables 
   WHERE table_schema NOT IN ('pg_catalog', 'information_schema')) as total_tables;

<DataTable data={direct_test} /> ```
```

