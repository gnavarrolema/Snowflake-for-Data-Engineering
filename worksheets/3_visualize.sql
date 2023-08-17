-- INVOICES TABLE
SELECT COUNT(DISTINCT COUNTRY) AS NUMBER_COUNTRIES FROM INVOICES;

-- TOP 2-10 countries with most clients
SELECT COUNTRY, 
       COUNT(DISTINCT CUSTOMERID) AS N_CLIENTS
   -- REMOVE UK AS IT HAS TOO MANY CLIENTS COMPARED TO OTHER COUNTRIES
FROM INVOICES
WHERE UPPER(COUNTRY) NOT LIKE 'UNITED%'
GROUP BY COUNTRY
ORDER BY N_CLIENTS DESC
LIMIT 10;

-- TOP clinets with most invoices
SELECT CUSTOMERID, COUNT(DISTINCT INVOICENO) AS N_ORDERS
FROM INVOICES
GROUP BY COUNTRY, CUSTOMERID
ORDER BY N_ORDERS DESC
LIMIT 10;

-- most ordered items
SELECT STOCKCODE,DESCRIPTION,SUM(QUANTITY) AS TOTAL_QUANTITY
FROM ITEMS
GROUP BY STOCKCODE, DESCRIPTION
ORDER BY TOTAL_QUANTITY DESC
LIMIT 10;



-- ITEMS TABLE

SELECT STOCKCODE, COUNT(DISTINCT INVOICENO)
FROM ITEMS
GROUP BY STOCKCODE;

-- overview of unit prices
WITH TEMP AS (
    SELECT DESCRIPTION, UNITPRICE
    FROM ITEMS
    GROUP BY STOCKCODE, DESCRIPTION, UNITPRICE
    ORDER BY UNITPRICE DESC)
SELECT COUNT(*), 
       AVG(UNITPRICE),
       MIN(UNITPRICE),
       MAX(UNITPRICE)
FROM TEMP;

--  Which customers bought a WHILE METAL LANTERN?
SELECT DISTINCT INVOICES.CUSTOMERID
FROM ITEMS
JOIN INVOICES ON ITEMS.INVOICENO=INVOICES.INVOICENO
WHERE ITEMS.DESCRIPTION = 'WHITE METAL LANTERN' 
AND INVOICES.CUSTOMERID IS NOT NULL;

-- Which ITEMS are the most revenue generating per country outside of UK?
SELECT ITEMS.DESCRIPTION, AVG(ITEMS.UNITPRICE) * SUM(ITEMS.QUANTITY) AS TOTAL_REVENUE, INVOICES.COUNTRY
FROM ITEMS
JOIN INVOICES ON ITEMS.INVOICENO=INVOICES.INVOICENO
WHERE UPPER(INVOICES.COUNTRY) NOT LIKE 'UNITED%'
GROUP BY ITEMS.DESCRIPTION, INVOICES.COUNTRY
ORDER BY TOTAL_REVENUE DESC, INVOICES.COUNTRY, ITEMS.DESCRIPTION;
