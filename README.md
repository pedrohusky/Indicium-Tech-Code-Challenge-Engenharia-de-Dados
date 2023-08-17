# Indicium Tech Code Challenge Engenharia de Dados

My solution for the Indicium Data Engineer technical test.

## Usage

There are multiple ways to use this pipeline:

1. **Command-Line Menu**: Run the pipeline with a command-line menu.
    ```
    python ./main.py
    ```

2. **Using Arguments**:
    - Reprocess data for a specific date:
        ```
        python ./main.py --reprocess (YYYY-MM-DD)
        ```
    - Execute steps sequentially:
        ```
        python ./main.py --sequentially
        ```
    - Execute steps individually by specifying a step number:
        ```
        python ./main.py --individually STEP_NUMBER
        ```
    - Run specific queries:
        ```
        python ./main.py --query
        ```

## Data Processing Steps

1. Data from sources is saved locally in JSON format: `data/postgres/employees/YYYY-MM-DD/employees.json`.
2. Data is loaded into memory and saved into a SQLite3 database.
3. The merged database is created by populating all tables and columns.
4. The merged database is saved as JSON: `merged_databases/merged_database_date-YYYY-MM-DD.json`.
5. Queries are saved in `query_output` with the format: `query_(QUERY_DATE)_generated_at_(DATE_IT_WAS_GENERATED)`.

## File Formats

- Processed data is saved in JSON format for its flexibility and usability.
- The merged database is saved in SQLite3 format because of my familiarity.

## Contact

For questions or feedback, feel free to reach out to me at: pedrohusky@live.com

