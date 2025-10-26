-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]
----------------------------------------------
-- SMARTGEAR SALES ANALYSIS FINAL SCRIPT
-- Consultant: Rahul Madhiwalla
-- Objective: Derive region, product, store & trend insights
----------------------------------------------

-- 1️⃣ Preview Data
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    HEADER_ROW = TRUE
)
WITH (
    OrderID INT,
    OrderDate VARCHAR(50),
    Region VARCHAR(100),
    StoreID INT,
    Product VARCHAR(150),
    Quantity INT,
    UnitPrice FLOAT
) AS SmartGearSales;


-- 2️⃣ Region-Wise Revenue and Units Sold
SELECT
    Region,
    SUM(Quantity * UnitPrice) AS Total_Revenue,
    SUM(Quantity) AS Units_Sold
FROM OPENROWSET(
    BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
    FORMAT='CSV',
    PARSER_VERSION='2.0',
    FIELDTERMINATOR=',',
    FIELDQUOTE='"',
    HEADER_ROW=TRUE
)
WITH (
    OrderID INT,
    OrderDate VARCHAR(50),
    Region VARCHAR(100),
    StoreID INT,
    Product VARCHAR(150),
    Quantity INT,
    UnitPrice FLOAT
) AS SmartGearSales
GROUP BY Region
ORDER BY Total_Revenue DESC;


-- 3️⃣ Product-Wise Performance
SELECT
    Product,
    SUM(Quantity * UnitPrice) AS Total_Revenue,
    SUM(Quantity) AS Units_Sold,
    ROUND(AVG(UnitPrice), 2) AS Avg_UnitPrice
FROM OPENROWSET(
    BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
    FORMAT='CSV',
    PARSER_VERSION='2.0',
    FIELDTERMINATOR=',',
    FIELDQUOTE='"',
    HEADER_ROW=TRUE
)
WITH (
    OrderID INT,
    OrderDate VARCHAR(50),
    Region VARCHAR(100),
    StoreID INT,
    Product VARCHAR(150),
    Quantity INT,
    UnitPrice FLOAT
) AS SmartGearSales
GROUP BY Product
ORDER BY Total_Revenue DESC;


-- 4️⃣ Top 10 Stores by Revenue
SELECT TOP 10
    StoreID,
    SUM(Quantity * UnitPrice) AS Total_Revenue,
    SUM(Quantity) AS Units_Sold
FROM OPENROWSET(
    BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
    FORMAT='CSV',
    PARSER_VERSION='2.0',
    FIELDTERMINATOR=',',
    FIELDQUOTE='"',
    HEADER_ROW=TRUE
)
WITH (
    OrderID INT,
    OrderDate VARCHAR(50),
    Region VARCHAR(100),
    StoreID INT,
    Product VARCHAR(150),
    Quantity INT,
    UnitPrice FLOAT
) AS SmartGearSales
GROUP BY StoreID
ORDER BY Total_Revenue DESC;


-- 5️⃣ Monthly Sales Trend
SELECT
    FORMAT(CAST(OrderDate AS DATE), 'yyyy-MM') AS Month,
    SUM(Quantity * UnitPrice) AS Monthly_Revenue
FROM OPENROWSET(
    BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
    FORMAT='CSV',
    PARSER_VERSION='2.0',
    FIELDTERMINATOR=',',
    FIELDQUOTE='"',
    HEADER_ROW=TRUE
)
WITH (
    OrderID INT,
    OrderDate VARCHAR(50),
    Region VARCHAR(100),
    StoreID INT,
    Product VARCHAR(150),
    Quantity INT,
    UnitPrice FLOAT
) AS SmartGearSales
GROUP BY FORMAT(CAST(OrderDate AS DATE), 'yyyy-MM')
ORDER BY Month;
-- 6. monthly sales trend diagnostic
SELECT DISTINCT OrderDate
FROM OPENROWSET(
  BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
  FORMAT='CSV', PARSER_VERSION='2.0',
  FIELDTERMINATOR=',', FIELDQUOTE='"',
  HEADER_ROW=TRUE
)
WITH (
  OrderID INT,
  OrderDate VARCHAR(200) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
  Region VARCHAR(100),
  StoreID INT,
  Product VARCHAR(150),
  Quantity INT,
  UnitPrice FLOAT
) AS T
WHERE OrderDate IS NOT NULL
  AND TRY_CONVERT(date, LTRIM(RTRIM(OrderDate)), 105) IS NULL  -- dd-mm-yyyy
  AND TRY_CONVERT(date, LTRIM(RTRIM(OrderDate)), 103) IS NULL  -- dd/mm/yyyy
  AND TRY_CONVERT(date, LTRIM(RTRIM(OrderDate)), 23) IS NULL   -- yyyy-mm-dd
  AND TRY_CONVERT(date, LTRIM(RTRIM(OrderDate)), 120) IS NULL  -- yyyy-mm-dd hh:mi:ss
  AND TRY_CAST(LEFT(LTRIM(RTRIM(OrderDate)),10) AS DATE) IS NULL;
 -- 7.since the diagnostic returned no issues the code is as bellow
 -- Robust Monthly Sales Trend (tries multiple formats)
WITH src AS (
  SELECT
    OrderID,
    LTRIM(RTRIM(OrderDate)) AS RawOrderDate,
    Region, StoreID, Product, Quantity, UnitPrice
  FROM OPENROWSET(
    BULK 'https://rahulapadsaccount.dfs.core.windows.net/synapsefs/smartgearsales.csv',
    FORMAT='CSV', PARSER_VERSION='2.0',
    FIELDTERMINATOR=',', FIELDQUOTE='"',
    HEADER_ROW=TRUE
  )
  WITH (
    OrderID INT,
    OrderDate VARCHAR(200) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    Region VARCHAR(100) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    StoreID INT,
    Product VARCHAR(150) COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    Quantity INT,
    UnitPrice FLOAT
  ) as t
)
, parsed AS (
  SELECT *,
    COALESCE(
      TRY_CONVERT(date, RawOrderDate, 105),  -- dd-mm-yyyy
      TRY_CONVERT(date, RawOrderDate, 103),  -- dd/mm/yyyy
      TRY_CONVERT(date, RawOrderDate, 23),   -- yyyy-mm-dd
      TRY_CONVERT(date, RawOrderDate, 120),  -- yyyy-mm-dd hh:mi:ss
      TRY_CAST(LEFT(RawOrderDate,10) AS DATE) -- first 10 chars, if they are yyyy-mm-dd or dd-mm-yyyy
    ) AS ParsedOrderDate
  FROM src
)
SELECT
  FORMAT(ParsedOrderDate, 'yyyy-MM') AS Month,
  SUM(TRY_CAST(Quantity AS BIGINT) * TRY_CAST(UnitPrice AS FLOAT)) AS Monthly_Revenue,
  SUM(TRY_CAST(Quantity AS BIGINT)) AS Units_Sold
FROM parsed
WHERE ParsedOrderDate IS NOT NULL
GROUP BY FORMAT(ParsedOrderDate, 'yyyy-MM')
ORDER BY Month;
