-- Order fulfillment pipeline extract.
-- Deliberately mixes temp tables, semicolon-free statements, and GO.
dEcLaRe @WarehouseId int = 42
declare @RunId uniqueidentifier = NEWID()

sEleCt
    o.OrderId,
    o.CustomerId,
    o.WarehouseId,
    o.PromiseDate,
    o.PriorityCode
into #OpenOrders
from sales.Orders as o
where
    o.WarehouseId = @WarehouseId
    and o.FulfillmentStatus in ('ALLOCATED', 'PICKING', 'PACKED')
    and o.CancelledAt is null

create index IX_OpenOrders_OrderId on #OpenOrders (OrderId)

gO

if exists (
    select
        1
    from #OpenOrders as oo
    where
        oo.PriorityCode = 'EXPRESS'
)
begin
    print 'Express orders detected for run';
end

insert into audit.FulfillmentRunLog (
    RunId,
    WarehouseId,
    OpenOrderCount,
    CreatedAt
)
select
    @RunId,
    @WarehouseId,
    COUNT_BIG(*),
    SYSUTCDATETIME()
from #OpenOrders as oo;

SeLeCt
    oo.OrderId,
    li.LineId,
    li.Sku,
    li.Quantity,
    inv.AvailableQuantity
from #OpenOrders as oo
inner join sales.OrderLines as li
    on li.OrderId = oo.OrderId
left join warehouse.Inventory as inv
    on inv.Sku = li.Sku
    and inv.WarehouseId = oo.WarehouseId
where
    COALESCE(inv.AvailableQuantity, 0) < li.Quantity
order by
    oo.PromiseDate,
    oo.PriorityCode desc;
