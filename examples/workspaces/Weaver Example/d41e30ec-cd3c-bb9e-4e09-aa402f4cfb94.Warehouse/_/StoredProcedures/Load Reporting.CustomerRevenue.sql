create   procedure [_].[Load Reporting.CustomerRevenue]
    @fault_tolerant bit = 0
  , @ignore_stability_threshold bit = 0
as
begin
    set nocount on;

    declare @weaver_load_datetime datetime2(6) = sysutcdatetime();
    declare @weaver_live_datetime datetime2(6) = convert(datetime2(6), '9999-12-31 23:59:59.999999');
    declare @weaver_rows_read bigint = 0;
    declare @weaver_rows_inserted bigint = 0;
    declare @weaver_rows_updated bigint = 0;
    declare @weaver_rows_deleted bigint = 0;
    declare @weaver_rows_rejected bigint = 0;
    declare @weaver_error varchar(4000) = null;
    declare @weaver_target_rows bigint = 0;
    declare @weaver_prospective_deletes bigint = 0;
    declare @weaver_prospective_updates bigint = 0;
    declare @weaver_target_before bigint = 0;

    /*-- Pre-processing --*/
    if object_id(N'[Reporting].[CustomerRevenue_Reject]', N'U') is not null drop table [Reporting].[CustomerRevenue_Reject];
    if object_id(N'[Reporting].[CustomerRevenue_Upsert]', N'U') is not null drop table [Reporting].[CustomerRevenue_Upsert];
    if object_id(N'[Reporting].[CustomerRevenue_Staging]', N'U') is not null drop table [Reporting].[CustomerRevenue_Staging];

    /*---- Data transformation ----*/
    create table [Reporting].[CustomerRevenue_Staging] as
    select
        s.*
      , row_number() over (partition by s.[Customer id] order by (select null)) as [__weaver_pk_row_number]
    from (
        select s.[Customer id]
             , s.[Customer name]
             , s.[Order count]
             , s.[Total amount]
          from [Reporting].[OrderSummary] s
    ) as s;
    /*---- End data transformation ----*/

    select @weaver_rows_read = count(*) from [Reporting].[CustomerRevenue_Staging];

    /*-- Post-processing --*/
    create table [Reporting].[CustomerRevenue_Reject] as
    select
        s.[Customer id]
      , s.[Customer name]
      , s.[Order count]
      , s.[Total amount]
      , case
            when nullif(trim(cast(s.[Customer id] as varchar(max))), '') is null then cast('blank_primary_key' as varchar(100))
            else cast('duplicate_primary_key' as varchar(100))
        end as [_reject_reason]
    from [Reporting].[CustomerRevenue_Staging] as s
    where nullif(trim(cast(s.[Customer id] as varchar(max))), '') is null
       or s.[__weaver_pk_row_number] > 1;

    select @weaver_rows_rejected = count(*) from [Reporting].[CustomerRevenue_Reject];

    delete s
    from [Reporting].[CustomerRevenue_Staging] as s
    where nullif(trim(cast(s.[Customer id] as varchar(max))), '') is null
       or s.[__weaver_pk_row_number] > 1;

    -- Intolerant of rejects: the target is left exactly as it was. Nothing has been
    -- written yet, so refusing here is a decision not to start rather than an
    -- unwind — and the reject table survives as the evidence.
    --
    -- Raised rather than returned, so `exec [_].[Load S.N]` fails the same way
    -- `.load()` does. A primitive that returned a quiet row where its sibling
    -- raised would make every caller special-case which one it was talking to.
    if @weaver_rows_rejected > 0 and @fault_tolerant = 0
        throw 51020, 'rows were rejected and fault_tolerant = 0, so the target was not modified', 1;

    create table [Reporting].[CustomerRevenue_Upsert] as
    select
        s.[Customer id]
      , s.[Customer name]
      , s.[Order count]
      , s.[Total amount]
      , case when t.[Customer id] is null then cast(1 as int) else cast(0 as int) end as [_Is new row]
    from [Reporting].[CustomerRevenue_Staging] as s
    left join [Reporting].[CustomerRevenue] as t on s.[Customer id] = t.[Customer id]
    where
        t.[Customer id] is null
        or exists (
            select s.[Customer id]
          , s.[Customer name]
          , s.[Order count]
          , s.[Total amount]
            except
            select t.[Customer id]
          , t.[Customer name]
          , t.[Order count]
          , t.[Total amount]
        );

    -- Everything this load is about to do, counted before it does any of it. A
    -- breach with @fault_tolerant = 0 leaves the target exactly as it was, so
    -- refusing is a decision not to start rather than an unwind.
    select @weaver_target_rows = count(*) from [Reporting].[CustomerRevenue];
    set @weaver_target_before = @weaver_target_rows;
    select @weaver_prospective_updates = count(*) from [Reporting].[CustomerRevenue_Upsert] where [_Is new row] = 0;
    select @weaver_prospective_deletes = count(*)
    from [Reporting].[CustomerRevenue] as c
    where not exists (
        select 1 from [Reporting].[CustomerRevenue_Staging] as s where s.[Customer id] = c.[Customer id]
    );

    if @ignore_stability_threshold = 0 and @weaver_target_rows > 0
        and @weaver_target_rows >= 1000000
    begin
        if @weaver_prospective_deletes * 100.0 / @weaver_target_rows > 5
            set @weaver_error = 'delete of ' + cast(@weaver_prospective_deletes as varchar(20))
                + ' rows is over the 5% threshold of '
                + cast(@weaver_target_rows as varchar(20));
        else if @weaver_prospective_updates * 100.0 / @weaver_target_rows > 20
            set @weaver_error = 'update of ' + cast(@weaver_prospective_updates as varchar(20))
                + ' rows is over the 20% threshold of '
                + cast(@weaver_target_rows as varchar(20));

        -- A breach never writes. Tolerating exactly the change the threshold was
        -- declared to prevent would defeat the guard, so @fault_tolerant decides
        -- only whether the refusal is raised or returned. Permitting it is what
        -- @ignore_stability_threshold is for.
        if @weaver_error is not null
        begin
            set @weaver_error = @weaver_error + '; the target was not modified';
            if @fault_tolerant = 0
                throw 51021, @weaver_error, 1;
            select
                cast(0 as bit) as succeeded
              , @weaver_rows_read as rows_read
              , cast(0 as bigint) as rows_inserted
              , cast(0 as bigint) as rows_updated
              , cast(0 as bigint) as rows_deleted
              , @weaver_rows_rejected as rows_rejected
              , @weaver_error as error_message;
            return;
        end;
    end;

    insert into [Reporting].[CustomerRevenue] (
        [Customer id]
      , [Customer name]
      , [Order count]
      , [Total amount]
      , [Row insert datetime]
      , [Row update datetime]
      , [Row delete datetime]
    )
    select
        u.[Customer id]
      , u.[Customer name]
      , u.[Order count]
      , u.[Total amount]
      , @weaver_load_datetime
      , @weaver_load_datetime
      , @weaver_live_datetime
    from [Reporting].[CustomerRevenue_Upsert] as u
    where u.[_Is new row] = 1;

    set @weaver_rows_inserted = @@rowcount;

    update c
    set
        c.[Customer name] = u.[Customer name]
      , c.[Order count] = u.[Order count]
      , c.[Total amount] = u.[Total amount]
      , c.[Row update datetime] = @weaver_load_datetime
      , c.[Row delete datetime] = @weaver_live_datetime
    from [Reporting].[CustomerRevenue] as c
    inner join [Reporting].[CustomerRevenue_Upsert] as u on c.[Customer id] = u.[Customer id]
    where u.[_Is new row] = 0;

    set @weaver_rows_updated = @@rowcount;

    delete c
    from [Reporting].[CustomerRevenue] as c
    where not exists (
        select 1 from [Reporting].[CustomerRevenue_Staging] as s where s.[Customer id] = c.[Customer id]
    );

    if @weaver_rows_rejected > 0 and @weaver_error is null
        set @weaver_error = cast(@weaver_rows_rejected as varchar(20))
            + ' rows were rejected and excluded from the load';

    if @weaver_rows_rejected = 0
    begin
        if object_id(N'[Reporting].[CustomerRevenue_Reject]', N'U') is not null drop table [Reporting].[CustomerRevenue_Reject];
        if object_id(N'[Reporting].[CustomerRevenue_Upsert]', N'U') is not null drop table [Reporting].[CustomerRevenue_Upsert];
        if object_id(N'[Reporting].[CustomerRevenue_Staging]', N'U') is not null drop table [Reporting].[CustomerRevenue_Staging];
    end;

    -- What the target actually lost, from its own cardinality. The delete
    -- driver says what the load intended; this says what happened, and the two
    -- differ whenever a key named for deletion was not there to begin with.
    select @weaver_rows_deleted =
        @weaver_target_before + @weaver_rows_inserted - count(*)
    from [Reporting].[CustomerRevenue];

    select
        cast(case when @weaver_error is null then 1 else 0 end as bit) as succeeded
      , @weaver_rows_read as rows_read
      , @weaver_rows_inserted as rows_inserted
      , @weaver_rows_updated as rows_updated
      , @weaver_rows_deleted as rows_deleted
      , @weaver_rows_rejected as rows_rejected
      , @weaver_error as error_message;
end;