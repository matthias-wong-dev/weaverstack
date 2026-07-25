/* Inventory replenishment forecast.
   Covers CROSS APPLY, subqueries in projection, temp tables, and OPTION hints. */
DECLARE @ForecastHorizonDays int = 28;
DECLARE @Today date = CONVERT(date, SYSUTCDATETIME());

SELECT
    sku.Sku,
    sku.CategoryCode,
    wh.WarehouseId,
    inv.OnHandQuantity,
    inv.ReservedQuantity,
    demand.ProjectedDemand,
    supplier.LeadTimeDays,
    (
        SELECT
            MAX(po.ExpectedReceiptDate)
        FROM purchasing.PurchaseOrders AS po
        WHERE
            po.Sku = sku.Sku
            AND po.WarehouseId = wh.WarehouseId
            AND po.StatusCode IN ('OPEN', 'PARTIAL')
    ) AS NextReceiptDate
INTO #Forecast
FROM masterdata.Skus AS sku
CROSS JOIN warehouse.Warehouses AS wh
INNER JOIN warehouse.InventoryBalance AS inv
    ON inv.Sku = sku.Sku
    AND inv.WarehouseId = wh.WarehouseId
CROSS APPLY (
    SELECT
        SUM(fc.ForecastQuantity) AS ProjectedDemand
    FROM planning.DailyDemandForecast AS fc
    WHERE
        fc.Sku = sku.Sku
        AND fc.WarehouseId = wh.WarehouseId
        AND fc.ForecastDate >= @Today
        AND fc.ForecastDate < DATEADD(day, @ForecastHorizonDays, @Today)
) AS demand
OUTER APPLY (
    SELECT TOP (1)
        s.LeadTimeDays
    FROM purchasing.SupplierSku AS s
    WHERE
        s.Sku = sku.Sku
        AND s.IsPreferred = 1
    ORDER BY
        s.PriorityRank,
        s.SupplierId
) AS supplier
WHERE
    sku.IsActive = 1
    AND wh.IsFulfillmentEnabled = 1
OPTION (RECOMPILE);

SELECT
    f.Sku,
    f.WarehouseId,
    f.ProjectedDemand,
    f.OnHandQuantity - f.ReservedQuantity AS AvailableQuantity,
    f.LeadTimeDays,
    f.NextReceiptDate
FROM #Forecast AS f
WHERE
    f.ProjectedDemand > (f.OnHandQuantity - f.ReservedQuantity)
ORDER BY
    f.ProjectedDemand DESC;
