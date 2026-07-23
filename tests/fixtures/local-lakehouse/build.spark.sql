-- Build: create the managed Delta tables empty, with declared schemas.
-- Stands in for what the build package will generate. {tables} is the target
-- Tables root, substituted by the test to an explicit path.
--
-- Column mapping is on because Weaver's declared column names contain spaces
-- (e.g. `Order id`), which Delta rejects otherwise. This is a real constraint
-- the build DDL will have to carry.

create table if not exists delta.`{tables}/Sales/Order` (
    `Order id`      string,
    `Customer id`   string,
    `Amount`        decimal(18,2)
) using delta
tblproperties (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion'   = '2',
    'delta.minWriterVersion'   = '5'
);

create table if not exists delta.`{tables}/Sales/Customer` (
    `Customer id`   string,
    `Customer name` string
) using delta
tblproperties (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion'   = '2',
    'delta.minWriterVersion'   = '5'
);
