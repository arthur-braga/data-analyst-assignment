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

To initiate this analysis, all tables were loaded to an AWS Redshift database. This step was done using a free trial AWS account, and all data wrangling operations were done using AWS's virtual SQL IDE.

Initially, there were some issues with the tables, as they were not previosuly cleaned. The files were loaded as .csv files, and all fields were set to the fields contained in the .csv filed. To stop from erroneously loading the header rows, an additional parameter was added. I've used this SQL operation:
`COPY dev.assignment.dimaccounts2 FROM 's3://dev-assignment-data-analyst/Data Analyst Assignment - DimAccounts.csv' IAM_ROLE 'arn:aws:iam::508300259937:role/devassignment' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2'`

As the data was loaded, AWS Redshift was able to identify what most columns were, so it was not necessary to alter column types from VARCHAR to different ones in most cases. Additionally, it was necessary to change few values which were supposed to be set as NULL, as AWS Redshift loaded them as a VARCHAR 'NULL'. In every single column that had this issue, I've used the following SQL operation:
`update assignment.dimaccounts da
set accounttype = NULL
where accounttype = 'NULL';`
In a few cases it was needed to add extra code, because some columns had values set to NA or just blank:
`update assignment.dimproduct dp
set color = NULL
where color = 'NULL' or color = 'NA';`
`update assignment.dimproduct dp
set status = NULL
where status = '';`
For date time fields, some fields were set to '00:00.0'. For this reason, there was a need for this additional code:
`update assignment.dimproduct dp
set enddate = NULL
where enddate = '00:00.0';`

AWS was not able to identify numeric and date time types. The former was not loaded with its decimal parts, and the latter had multiple issues, as stated below. For that, I've labelled all such variables as VARCHAR, and manually adjusted them. I've used the following SQL operations:
`alter table assignment.dimproduct add column standardcost_new numeric(10,6);
update assignment.dimproduct 
set standardcost_new = cast(standardcost as numeric(10,6));
alter table assignment.dimproduct drop column standardcost;
alter table assignment.dimproduct rename column standardcost_new to standardcost;
select * from assignment.dimproduct dp;`
For date time fields, I've used the following code:
`alter table assignment.dimproduct add column enddate_new timestamp;
update assignment.dimproduct 
set enddate_new = cast(enddate as timestamp);
alter table assignment.dimproduct drop column enddate;
alter table assignment.dimproduct rename column enddate_new to enddate;
select * from assignment.dimproduct dp;`
It also came to my attention that the table assingment.dimproduct had an issue with the fields startdate and enddate. Every single row with non-null values for both fields had a startdate with a higher value than the enddate. This happened 57 times. To check this, I've used this operation: `select count(*) from assignment.dimproduct dp where enddate < startdate;`. However, there are 229 rows where there is a value for startdate, but none for enddate. I've used this code to check this: `select count(*) from assignment.dimproduct dp where enddate is null and startdate is not null;` There was not any row where there was a value for enddate and not a value for startdate. This prompts multiple questions about what is going on. At first, one might think that the've switched the field names by mistake. But, as there are rows with starting dates and no ending dates, it has to be considered two options: i) the fields are switched for all rows, thus some products have only ending dates and we know theses products are no longer manufactured; or ii) the fields are switched only for rows where there are values for both startdate and enddate, thus for some products we know where they began being manufactured, and we can assume they are still being manufactured. As it is unlikely that someone just switched a few rows by accident, instead of the field names, I am going to consider the first hypothesis, and just switch the field names. Addtionally, there is a field called finishedgoodsflag, and every row with a stardate and enddate has the finishedgoodsflag set to 1, and every row with a finishedgoodsflag set to 0 had a startdate and no enddate, except for one row where the both values are null. This is verifiable using these codes: `select finishedgoodsflag, startdate, enddate from assignment.dimproduct dp where finishedgoodsflag = 1;`; `select finishedgoodsflag, startdate, enddate from assignment.dimproduct dp where finishedgoodsflag = 0;`. Although this could have helped, it is necessary further explanation of the data to make a better assumption. To change filed names, I've used the following operation:
`alter table assignment.dimproduct rename column startdate to enddate_new;
alter table assignment.dimproduct rename column enddate to startdate;
alter table assignment.dimproduct rename column enddate_new to enddate;
select * from assignment.dimproduct dp;`

Looking at the table dimcustomer, I've found out that there is a field called namestyle, but all of its values are set to 0. I am not sure what it means, but at least all values are the same. I've used this code: `select distinct namestyle from assignment.dimcustomer;`. This table had date columns loaded as string in the DD/MM/YY format. To adjust this I've used the following operation:
`alter table assignment.dimcustomer add column birthdate_new date;
update assignment.dimcustomer 
set birthdate_new = cast(to_date(birthdate, 'DD/MM/YY') as date);
alter table assignment.dimcustomer drop column birthdate;
alter table assignment.dimcustomer rename column birthdate_new to birthdate;`
It also came to my attention that some birthdates don't make sense. I found that 8400 customers had birthdates that happen to be in the future, i.e. later than today's date. I've checked it with this code: `select count(*) from (select dc.birthdate, datediff('year', dc.birthdate, current_date) as age from assignment.dimcustomer dc) where age < 0`. If I compare it with the field datefirstpurchase, I can see that this happens 8412 times. What seems to be the issue is that the information was originally sotred as 'DD/MM/YY' and, as I converted it to a date field, AWS Redshift misinterpreted it. As most data come from the yearly 2010's, it just does not make sense to consider that any person was born after the turn of the millenia. Hence, I will change every single row that is after 2000 to the same value, but 100 years earlier. Thus, someone with the birthdate set to December 4, 2013, will turn into to December 4, 1913. To do so, I've used the following coe:
`update assignment.dimcustomer
set birthdate = dateadd(year, -100, birthdate)
where birthdate >= '2000-01-01';`

The table factfinance also had date columns, and I've did the same operations to adjust them. There was a "date" column, and a datekey column. As the second one is a key column, I've left it as an integer. It is important to notice that the "date" column had what looked like a date time type information. As this is labelled as a date field, and all hour information was '00:00.0', I've set that field as a date column. The table factresellersales had the exact same issue with datekeys columns and date time columns with no hour information.

After this initial analysis and adjusment of the data, I will start with the questions.

1 - Find the highest transaction of each month in 2012 for the product Sport-100 Helmet, Red

First, I have to figure which productkey this 'Sport-100 Helmet, Red' is. For that, I've used the following code: `select * from assignment.dimproduct dp where dp.productname like '%Sport-100%'`. With this, I've found that there are three productkeys with that name: 212, 213, and 214. There is the productalternatekey that encompasses all three entries for 'Sport-100 Helmet', but it is better to use productkey because the other table I need information from contains only the column productkey. A quick check with the code `select * from assignment.dimproduct dp where dp.productkey in (212, 213, 214) order by dp.productname` asserts me that these productkeys are only used for the 'Sport-100 Helmet'.

To fetch the data, I've decided to use the rank() window function, as it allows me to partition by month and order by the highest sales amount. Lastly, to present the view as month, I've altered the date format to display the actual month names, together with the year. I believe this creates a better understaing for the viewer, specially because the 'YYYY-MM-DD' is not really friendly for non-technical folk. This was my query:
`create or replace view assignment.sales_sport_100_helmet_red_2012 as
with sales as (
  select
      to_char(date_trunc('month', frs.orderdate), 'MONTH YYYY') as Month,
	  extract(month from date_trunc('month', frs.orderdate)) as month_order,
      rank() over(partition by Month order by frs.salesamount desc) as rnk,
      frs.salesamount as SalesAmount,
      frs.orderdate AS OrderDate
  from
      assignment.factresellersales frs
  where
      1 = 1
      and frs.productkey in (212, 213, 214)
      and frs.orderdate between '2012-01-01' and '2012-12-31'
  order by month_order
)
select
	Month,
    SalesAmount,
    OrderDate
from 
	sales
where rnk = 1`

2 - Find the lowest revenue-generating product for each month in **2012**. Include the **Sales Territory Country** as well.

The approach to this question is quite similar, the main difference is that we are now looking for the revenue generates by a product, and also the lowest amount. This implies the total value that each product generated, not just the value of a single transaction. To solve this, I've created the following view:
`create or replace view assignment.lowest_revenue_generating_product_2012 as
with sales as (
  select distinct
      to_char(date_trunc('month', frs.orderdate), 'MONTH YYYY') as Month,
	  extract(month from date_trunc('month', frs.orderdate)) as month_order,
      rank() over(partition by Month order by sum(frs.salesamount)) as rnk,
      sum(frs.salesamount) as SalesAmount,
      dp.productname,
  	  dst.salesterritorycountry
  from
      assignment.factresellersales frs
  left join assignment.dimproduct dp on
  	frs.productkey = dp.productkey
  left join assignment.dimsalesterritory dst on
  	frs.salesterritorykey = dst.salesterritorykey
  where
      1 = 1
      and frs.orderdate between '2012-01-01' and '2012-12-31'
  group by 1, 2, 5, 6
  order by month_order
)
select
	Month,
    salesterritorycountry,
    productname,
    SalesAmount
from 
	sales
where rnk = 1;`

3 - Find the Average Finance Amount for each Scenario (Actual Scenario, Budget Scenario, Forecast Scenario) for each Account Type (Assets, Balances, Liabilities, Flow, Expenditures, Revenue) in 2011.

To beign with, it is necessary to assert which ones are the scenarios. With the following code I can get this information: `select distinct scenarioname, scenariokey from assignment.dimscenario`. It is important to note that the table factfinance only contains scenarios 1 and 2, that is, actual scenario and budget scenario. To get the same information for account types, I've used this code: `select distinct accounttype, accountkey from assignment.dimaccounts order by accounttype`. I can see that there are several account keys for the same account type. Hence, the final query has to account this for. To arrange the view as requested, I've decided to use the pivot function, as it can pivot data contained in multiple rows of a single column and change it to different rows. Lastly, I've changed the scenariokey values to the requested value, and I believe it improves readability. At the end, I wrote the following code:
`create or replace view assignment.average_finance_amount_scenario as
select accounttype, "1" as "actual scenario", "2" as "budget scenario", "3" as "forecast scenario" from (
  select distinct
      da.accounttype,
      ff.scenariokey,
  	  ff.amount as salesamount
  from
      assignment.factfinance ff
  left join assignment.dimaccounts da on
      ff.accountkey = da.accountkey
  where "date" between '2011-01-01' and '2011-12-31'
) pivot (avg(salesamount) for scenariokey in (1, 2, 3)
);`

4 - Find all the products and their Total Sales Amount by Month of order which does have sales in 2012.

This is simillar to questions 1 and 2. However, there is no need to use the rank() window function, thus it was somewhat a reduction of what I had written for question 2. This is the final code:
`create or replace view assignment.product_sales_2012 as
with sales as (
  select distinct
      to_char(date_trunc('month', frs.orderdate), 'MONTH YYYY') as Month,
	  extract(month from date_trunc('month', frs.orderdate)) as month_order,
      sum(frs.salesamount) as SalesAmount,
      dp.productkey
  from
      assignment.factresellersales frs
  left join assignment.dimproduct dp on
  	frs.productkey = dp.productkey
  where
      1 = 1
      and frs.orderdate between '2012-01-01' and '2012-12-31'
  group by 1, 2, 4
  order by productkey, month_order
)
select
	productkey,
    SalesAmount,
    Month as ordermonth
from 
	sales`
 
5 -  Write a query to find the age of customers. Bucket them under **Age Group**:Less than 35; Between 35 and 50; and Greater than 50. Segregate the Number of Customers in each age group on **Marital Status** and **Gender**.

The first step for this task is to determine the age group. This is possible by using the datediff function, which lets us compare the age of customers as compared to today's date. However, I believe this database is quite old, as the customers have aged and, in 2022, there is no one that is under 35 years old. To be 35 years old or less, the person should be born after 1987, and there is no one in such conditions. Using `select max(orderdate) from assignment.factresellersales`, I can see that the most recent order date is November 29, 2013. If we take that date into consideration, we would have 4388 people with less than 35 years old. Nevertheless, I am going to stick with current age, thus there won't be anyone aged below 35.
To write the query, I've used the pivot function again, this time to create the columns for the age groups. However, this time this operation was not enough, so I've inserted it into a subquery, so I can agreggate the sum of the values for each age group. The result is the following code:
`create or replace view assignment.customers_age_marital_gender as
select
	case when maritalstatus = 'M' then 'Married' else 'Single' end as maritalstatus,
    case when gender = 'M' then 'Male' else 'Female' end as gender,
    sum("1") as "Age < 35",
    sum("2") as "Age between 35-50",
    sum("3") as "Age > 50"
from (
  select * from (
      select
        dc.birthdate,
        dc.maritalstatus,
        dc.gender,
        datediff('year', dc.birthdate, current_date) as age,
        case
          when age < 35 then 1
          when age >= 35 and age < 50 then 2
          when age >= 50 then 3
          else null
          end as age_group
      from
          assignment.dimcustomer dc
  ) pivot (count(*) for age_group in (1, 2, 3))
) group by 1, 2 order by 1, 2;`

6 - Based on your results for question #2 above, create a visualization to highlight the sales territories with the lowest sales performances. Are there any territories with consistent low sales performance over time?

The initial analysis of the lowest revenue-generating product of 2012 found that  the AWC Logo Cap was the most frequent lowes revenue-generating product of 2012, with three entries for the monts of February, May, and June. However, even more frequent than it was the United Kingdom. As country of sales territory, the UK was by far the most frequent country, with eight entries. That means that two thirds of the year this country sold the lowest revenue-generating product. The second country was France, with three entries. It is important to notice that this initial analysis only considers data from 2012. For this task, we have to take into consideration the whole dataset, which also adds more data about the Australian and German markets, which only began in the end of 2012.

The European and Australian markets as a whole have consistently the weakest sales performance of all regions in comparison with the North American market, specially the US, being Australia the weakest one. This indicates that there is a significant difference in Europe and Australia, whereas in North America the market is more established. Comparing the revenue by region, we can assert that the American market, the best one by far, is actually comprised by several regions. Thus, the conclusion is not that in Europe and Australia sales are weak, but that the US and Canada are far bigger countries, with multiple regions, so it makes more sense to think in regions, not countries. Even though Australia is also big, it is a relatively new market, thus the sales in Australia are likely contained within on region of the country.

7 - Create a visualization based on your results for question #3 above, so that the user can switch between scenarios and account types. Please explain what insight can we gain from these results.

This visualization will take into consideration that there only exists current and budget scenario. It is possible to choose the account type, the scenario name, and the date parameters, which can be the period and the date part, i.e. day, month or year.

The data was presented in a table to facilitate the readability of the data. However, a time series was also added, following the same parameters, so it is possible to visually interpret the tabular data, specially in situations where the date parameters compromise the readability.

Both visualizations allow the viewer not only to compare different scenarios and account types, but also visualize how each figure change over time. There is an increased level of possibilities, as all date parameters are customizable.

8 - Create a visualization based on your results for question #5 above. Please explain what insight can we gain from these results.

Initially analysis focused on the age group of less than 35 years of age, between 35 and 50, and greater 50 years of age. However, that took into consideration today, June 2022. The dataset ends at the end of 2013, thus it would make more sense to limit the age group calculation to 2013, so we would have people in all age groups. If we took into consideration today as reference, we would have no one in the less than 35 years age group, because the youngest customer was born in 1986 (which makes they 36 years old).

The analysis below demonstrantes that there are more married customers in age groups between 35 and 50, and greater than 50 years. Additionally, there is a predominance of male customers in all groups, except for single customers over 50, which suggests that there are more female widow customers. The median and the mean age of customers between groups is similar, suggesting that the distribution of age between groups is similar to the normal distribution,  which is expected for age variables.

In terms of education and occupation, the difference between age groups is quite similar to what is expected, that is, the older age groups have a higher proportion of higher education and professional and management occupations.

Finally, the last plot tells us that the age group between 35 and 50 is the predominant one in terms of new customers throughout the dataset, suggesting that age group are more likely to become new customers than all other groups. Additionally, we can see that there was an influx of new customers in early 2013, possibly due to the new markets of Germany and Australia. Finally, we can see that the age groups of lees than 35 and greater 50 were quite similar in terms of new customers, but that changed in late 2012, which suggests that after this period the company had more eligible senior customers.
