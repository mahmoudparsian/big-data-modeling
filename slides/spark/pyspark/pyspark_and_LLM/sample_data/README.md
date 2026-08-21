# sample_data

Small CSV datasets used by the `progs_row_level`/`progs_table_level` PySpark + LLM examples.

## Contents

| Name | Type | Description |
|---|---|---|
| [`data.csv`](data.csv) | csv (212B) | Employee records: `name,age,department,salary` |
| [`departments.csv`](departments.csv) | csv (58B) | Department lookup table: `id,name` |
| [`employees.csv`](employees.csv) | csv (185B) | Employee records with a `department_id` FK, for joining against `departments.csv` |
