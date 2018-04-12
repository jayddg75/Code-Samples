/*------------------------------------------------------------------------------------------------
Created By: Jason Schilli
Create Date: 4/18/2018

Proc Info:  This proc will create a file for the top 10 most profitable customers of
all time of monthy sales metrics for trending.  Need to include zero records for months
in which a customer has no sales.

This would be for used for spreasheet analysis, not for someone with an ad-hoc reporting 
tool like Tableau.




------------------------------------------------------------------------------------------------*/


-- get the monthly totals of all orders, including months with none and write out a 

use WideWorldImporters


declare @cnt int = 1;


-- create the list of the the top 10 customers of all time
if object_id('tempdb..#cust') is not null
	drop table #cust


select top 10 c.CustomerID
	,c.CustomerName
	,max(year(i.InvoiceDate)) as MaxYear 
	,min(year(i.InvoiceDate)) as MinYear
into #cust
from Sales.Invoices i
join Sales.InvoiceLines il on i.InvoiceID = il.InvoiceID
join Sales.Customers c on i.CustomerID = c.CustomerID
group by c.CustomerID, c.CustomerName
order by sum(il.LineProfit) desc





------------------------------------------------------------------------------------------
-- create the list of months and years for the time frame needed


if object_id('tempdb..#months') is not null
	drop table #months


create table #months (
MonthNum int
);



while @cnt < 13
begin

	insert into #months values (@cnt)

	set @cnt += 1

end



-- make sure you have all years covered that you have invoices for ...
if object_id('tempdb..#years') is not null
	drop table #years


create table #years (
YearNum int
);


-- set the initial value to the smallest min year from #cust
set @cnt = ( select min(MinYear) from #cust )


while @cnt <= ( select max(MaxYear) from #cust )
begin

	insert into #years values (@cnt)

	set @cnt += 1

end




------------------------------------------------------------------------------------------
-- create the base listing of all months, years, and customers

if object_id('tempdb..#finalout') is not null
	drop table #finalout


create table #finalout (
MonthStart date,
CustomerId int,
CustomerName varchar(100),
Quanity int,
UnitPrice money,
TaxAmount money,
LineProfit money,
ExtendedPrice money
);


insert into #finalout (MonthStart, CustomerId, CustomerName)

	select datefromparts(YearNum, MonthNum, 1) 
		,b.CustomerId
		,b.CustomerName 
	from #years
	cross apply #months
	cross apply ( 
	
		select CustomerId, CustomerName 
		from #cust 
		
	) b






-- update the #finalout table with all the metrics and then fill in the missing months with zeroes
-- you can use the merge statment to update the non-matches at the same time as the matches
-- otherwise you are writing two update statments, which could be slower


merge into #finalout t
using (

	select CustomerId
		,dateadd(dd, -datepart(dd, i.InvoiceDate) + 1, i.InvoiceDate) as MonthStart
		,sum(il.Quantity) as Quanity
		,sum(il.UnitPrice) as UnitPrice
		,sum(il.TaxAmount) as TaxAmount
		,sum(il.LineProfit) as LineProfit
		,sum(il.ExtendedPrice) as ExtendedPrice
	from Sales.Invoices i
	join Sales.InvoiceLines il on i.InvoiceID = il.InvoiceID
	where CustomerId in ( select CustomerId from #cust )
	group by CustomerId, dateadd(dd, -datepart(dd, i.InvoiceDate) + 1, i.InvoiceDate)

) s
on t.CustomerId = s.CustomerID
	and t.MonthStart = s.MonthStart

when matched then update set
	 t.Quanity = s.Quanity
	,t.UnitPrice = s.UnitPrice
	,t.TaxAmount = s.TaxAmount
	,t.LineProfit = s.LineProfit
	,t.ExtendedPrice = s.ExtendedPrice

when not matched by source then update set
	 t.Quanity = 0
	,t.UnitPrice = 0
	,t.TaxAmount = 0
	,t.LineProfit = 0
	,t.ExtendedPrice = 0;






-- loop through each customer and write out a pipe delimited file ...


-- create the table to export from

if object_id('Export') is not null
	drop table Export

select top 0 *
into Export
from #finalout




declare @CustomerId int
	,@BCPString varchar(1000);

declare Cust cursor for

	select CustomerId
	from #cust;



open Cust;

fetch next from Cust into @CustomerId

while @@FETCH_STATUS = 0  
begin 


	-- truncate the table, insert the new records, then bcp them out.
	truncate table export

	insert into export

		select *
		from #finalout
		where CustomerId = @CustomerId


	set @BCPString = 'bcp WideWorldImporters.dbo.export out C:\FileDropoff\' + cast(@CustomerId as varchar(100)) + '.txt -t"|" -c -T '

	select @BCPString
	-- export out the pipe delimted file
	execute master.dbo.xp_cmdshell @BCPString


	fetch next from Cust into @CustomerId

end

close Cust;
deallocate Cust;


