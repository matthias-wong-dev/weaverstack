/* Month-end revenue reconciliation script.
   Includes aggregates, UNION ALL, HAVING, table variables, and an UPDATE. */
DECLARE @PeriodStart date = '2026-05-01';
DECLARE @PeriodEnd date = EOMONTH(@PeriodStart);

DECLARE @Ledger TABLE (
    SourceSystem varchar(30) NOT NULL,
    AccountCode varchar(30) NOT NULL,
    Amount money NOT NULL
);

INSERT INTO @Ledger (SourceSystem, AccountCode, Amount)
SELECT
    'Billing',
    b.AccountCode,
    SUM(b.NetAmount)
FROM billing.InvoiceLines AS b
WHERE
    b.InvoiceDate >= @PeriodStart
    AND b.InvoiceDate < DATEADD(day, 1, @PeriodEnd)
GROUP BY
    b.AccountCode
HAVING
    SUM(b.NetAmount) <> 0
UNION ALL
SELECT
    'Payments',
    p.AccountCode,
    SUM(p.AmountApplied * -1)
FROM payments.Applications AS p
WHERE
    p.AppliedDate >= @PeriodStart
    AND p.AppliedDate < DATEADD(day, 1, @PeriodEnd)
GROUP BY
    p.AccountCode;

UPDATE finance.PeriodCloseStatus
SET LastCheckedAt = SYSUTCDATETIME()
WHERE PeriodStart = @PeriodStart;

SELECT
    'ACCOUNT' AS RowType,
    l.AccountCode,
    SUM(CASE WHEN l.SourceSystem = 'Billing' THEN l.Amount ELSE 0 END) AS BillingAmount,
    SUM(CASE WHEN l.SourceSystem = 'Payments' THEN l.Amount ELSE 0 END) AS PaymentAmount,
    SUM(l.Amount) AS Difference
FROM @Ledger AS l
GROUP BY
    l.AccountCode
HAVING
    ABS(SUM(l.Amount)) > 0.01
UNION ALL
SELECT
    'TOTAL' AS RowType,
    NULL AS AccountCode,
    SUM(CASE WHEN l.SourceSystem = 'Billing' THEN l.Amount ELSE 0 END) AS BillingAmount,
    SUM(CASE WHEN l.SourceSystem = 'Payments' THEN l.Amount ELSE 0 END) AS PaymentAmount,
    SUM(l.Amount) AS Difference
FROM @Ledger AS l
ORDER BY
    RowType,
    ABS(Difference) DESC
