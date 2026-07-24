-- Load: put rows into the tables the build created. Stands in for what a
-- generated load program will do. Idempotent by overwrite, so a rebuild+reload
-- recovers cleanly.

insert overwrite delta.`{tables}/Sales/Order` values
    ('A1', 'C1', 10.00),
    ('A2', 'C1', 20.00),
    ('A3', 'C2', 30.00);

insert overwrite delta.`{tables}/Sales/Customer` values
    ('C1', 'Ackland'),
    ('C2', 'Beattie');

insert overwrite delta.`{tables}/Reporting/OrderSummary` values
    ('C1', 2, 30.00),
    ('C2', 1, 30.00);
