/* Customer retention cohort report.
   Uses variables, CTEs, window functions, EXISTS predicates, and final ordering. */
declare @StartDate date = '2026-01-01';
dEcLaRe @EndDate date = '2026-03-31';
DECLARE @MinimumOrders int = 2;

with base_customers as (
    SeLeCt
        c.CustomerId,
        c.AccountNumber,
        c.CreatedAt,
        c.RegionCode
    fRoM dbo.Customers as c with (NOLOCK)
    wHeRe
        c.CreatedAt < DATEADD(day, 1, @EndDate)
        aNd c.IsTestAccount = 0
),
orders_in_window as (
    select
        o.CustomerId,
        COUNT_BIG(*) as OrderCount,
        MIN(o.OrderDate) as FirstOrderDate,
        MAX(o.OrderDate) as LastOrderDate
    from sales.Orders as o
    where
        o.OrderDate >= @StartDate
        and o.OrderDate < DATEADD(day, 1, @EndDate)
        and o.StatusCode not in ('CANCELLED', 'FRAUD')
    group by
        o.CustomerId
),
ranked_customers as (
    SELECT
        bc.CustomerId,
        bc.AccountNumber,
        bc.RegionCode,
        oi.OrderCount,
        oi.FirstOrderDate,
        oi.LastOrderDate,
        ROW_NUMBER() over (
            partition by bc.RegionCode
            order by oi.OrderCount desc, oi.LastOrderDate DESC
        ) as RegionRank
    from base_customers as bc
    inner join orders_in_window as oi
        ON oi.CustomerId = bc.CustomerId
    WHERE
        oi.OrderCount >= @MinimumOrders
        and exists (
            sElEcT
                1
            FrOm crm.CustomerSubscriptions as cs
            WhErE
                cs.CustomerId = bc.CustomerId
                and cs.IsActive = 1
        )
)
select
    rc.CustomerId,
    rc.AccountNumber,
    rc.RegionCode,
    rc.OrderCount,
    rc.FirstOrderDate,
    rc.LastOrderDate,
    rc.RegionRank
from ranked_customers as rc
where
    rc.RegionRank <= 100
order by
    rc.RegionCode,
    rc.RegionRank;
