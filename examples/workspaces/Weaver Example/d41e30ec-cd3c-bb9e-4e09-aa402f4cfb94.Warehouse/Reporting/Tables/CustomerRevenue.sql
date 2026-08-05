CREATE TABLE [Reporting].[CustomerRevenue] (

	[Customer key] bigint IDENTITY NOT NULL, 
	[Customer id] varchar(50) NOT NULL, 
	[Customer name] varchar(200) NULL, 
	[Order count] bigint NULL, 
	[Total amount] decimal(18,2) NULL, 
	[Row insert datetime] datetime2(6) NOT NULL, 
	[Row update datetime] datetime2(6) NOT NULL, 
	[Row delete datetime] datetime2(6) NOT NULL
);


GO
ALTER TABLE [Reporting].[CustomerRevenue] ADD CONSTRAINT PK_CustomerRevenue primary key NONCLUSTERED ([Customer id]);