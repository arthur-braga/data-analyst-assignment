# data-analyst-assignment

Data Analyst Assignment
Arthur Braga

This README documents the analysis of the following seven different tables, being five dimensional tables, and two fact tables:

- Dim Accounts;
- Dim Customer;
- Dim Product;
- Dim Sales Territory;
- Dim Scenario;
- Fact Finance; and
- Fact Reseller Sales.

To initiate this analysis, all tables were loaded to an AWS Redshift database. This step was done using a free trial AWS account, and all data wrangling operations were done using AWS's virtual IDE.

Initially, there were some issues with the tables, as they were not previosuly cleaned. The files were loaded as .csv files, and all fields were set to VARCHAR. The tables Dim Accounts and Dim Product were erroneously loaded with the first row being the column-names, and these were properly dropped. All other tables did not have such issues.

To remove these rows, I've used this SQL operation:
`delete from assignment.dimproduct where productkey = 'ProductKey'`.

As the data was loaded, AWS Redshift was able to identify what each column was, so it was not necessary to alter column types from VARCHAR to different ones, like INT for example. However, it was necessary to change few values which were supposed to be set as NULL, but AWS Redshift loaded them as a VARCHAR 'NULL'. In every single column from every single row, Ive used the following SQL operation:
`update assignment.dimaccounts da
set accounttype = NULL
where accounttype = 'NULL';`
