# Internet Sales ETL Program
#### Author: Dennis Kim

## Configurations:
Use config.json file to indicate configuration settings of your choosing. 
## Example terminal command line:
#### Extraction step
python ./apps/etldata/src/etldata.py -input ./data/input_ecomm_sales.csv -process config_extraction -output ./data/joinedData.xlsx -mapping ./data/mapping_ecomm_sales.xlsx -log ./apps/etldata/src/etldata.log
#### Transformation step
python ./apps/etldata/src/etldata.py -input ./data/joinedData.xlsx -process config_transformation -output ./data/transformedData.xlsx -mapping ./data/mapping_ecomm_sales.xlsx -log .\apps\etldata\src\etldata.log

## Workflow:
#### Extraction process
1) Reads in sales figures
2) Conducts a left join of sales figures and country-to-region mapping
3) Makes column adjustments (changing column names/adding static columns)
4) Writes to file using filepath provided

#### Transformation process
1) Reads in output of extraction process
2) Transforms data using pivot or groupby techniques to calculate total revenue and percentage of total revenue by elected column
3) Adds calculations as extra columns
4) Writes resulting dataframe to filepath provided

## Data source:
https://archive.ics.uci.edu/ml/datasets/Online+Retail+II





