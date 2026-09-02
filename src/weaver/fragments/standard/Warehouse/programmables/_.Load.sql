/*
The generic load entry point. `exec [_].[Load] @object_name = 'Sales.Customer'`
runs [_].[Load Sales.Customer], maps what it returned into the catalogue's
Result vocabulary, writes the operational record, and raises again anything the
procedure raised.

The implementation procedure is in [_], and the source object's schema is part
of its name. `@item_name` omitted means recover it from [_].[Installation].

`@reload = 1` reconstructs the object from zero. This ends its load state first,
putting [_].[LoadStatus] to Pending and removing the [_].[Bookmark] row, and the
implementation procedure then empties the target and runs. That order is what
keeps a bookmark from standing over rows that are gone. It reaches this object
and nothing else: a consumer of it loads next as it always would.

Every write is a MERGE, including the appends. In every Warehouse but the one
the catalogue lives in these tables are views across databases, and Fabric
refuses a plain INSERT through such a view while accepting a MERGE's. An
appended row merges on a surrogate generated a moment ago, so it never matches.

Weaver-owned content. See weaver/fragments and design/catalogue.md.
*/
create or alter procedure [_].[Load]
    @object_name varchar(261)
  , @fault_tolerant bit = 0
  , @ignore_stability_threshold bit = 0
  , @reload bit = 0
  , @item_name varchar(128) = null
as
begin
    set nocount on;

    declare @weaver_unmatched nvarchar(2048) = null;
    declare @weaver_started datetime2(6) = sysutcdatetime();
    declare @weaver_completed datetime2(6) = null;
    declare @weaver_workflow varchar(128) = cast(newid() as varchar(36));
    declare @weaver_log_sk varchar(128) = cast(newid() as varchar(36));
    declare @weaver_schema varchar(128) = null;
    declare @weaver_object varchar(128) = null;
    declare @weaver_target nvarchar(261) = null;
    declare @weaver_call nvarchar(max) = null;
    declare @weaver_error varchar(4000) = null;
    declare @weaver_error_number int = null;
    declare @weaver_rethrow nvarchar(2048) = null;
    declare @weaver_result varchar(128) = null;
    declare @weaver_message varchar(4000) = null;
    declare @weaver_statistic_sk varchar(128) = cast(newid() as varchar(36));
    declare @succeeded bit = null;
    declare @rows_read bigint = null;
    declare @rows_inserted bigint = null;
    declare @rows_updated bigint = null;
    declare @rows_deleted bigint = null;
    declare @rows_rejected bigint = null;
    declare @error_message varchar(4000) = null;
    declare @bookmark_datetime datetime2(6) = null;
    declare @is_static_skip bit = null;

    if charindex('.', @object_name) = 0
    begin
        set @weaver_unmatched = concat(@object_name, N' is not a Schema.Object name');
    end
    else
    begin
        set @weaver_schema = substring(@object_name, 1, charindex('.', @object_name) - 1);
        set @weaver_object = substring(@object_name, charindex('.', @object_name) + 1, len(@object_name));
    end;

    if @item_name is null and @weaver_unmatched is null
    begin
        declare @weaver_installations int;
        select @weaver_installations = count(*)
             , @item_name = min([Item name])
          from [_].[Installation]
         where [Item type] = N'Warehouse'
           and [Target name] = db_name();
        if @weaver_installations <> 1
        begin
            set @weaver_unmatched = concat(
                N'the logical Weaver item for ', db_name(),
                N' is not unique in _.Installation; supply @item_name');
            set @item_name = null;
        end;
    end;

    begin try
        if @weaver_unmatched is null
        begin
            set @weaver_target = N'[_].[' + replace(N'Load ' + @object_name, N']', N']]') + N']';
            if object_id(@weaver_target, 'P') is null
            begin
                set @weaver_unmatched = concat(@object_name,
                    N' is not a loadable object in this Warehouse');
            end
            else
            begin
                -- Both halves are the dynamic batch's own names: the left is
                -- the implementation procedure's parameter, the right the
                -- sp_executesql parameter bound to the outer variable below.
                set @weaver_call = N'exec ' + @weaver_target + N' '
                    + N'@fault_tolerant = @fault_tolerant'
                    + N', @ignore_stability_threshold = @ignore_stability_threshold'
                    + N', @reload = @reload'
                    + N', @weaver_succeeded = @weaver_succeeded output'
                    + N', @weaver_rows_read = @weaver_rows_read output'
                    + N', @weaver_rows_inserted = @weaver_rows_inserted output'
                    + N', @weaver_rows_updated = @weaver_rows_updated output'
                    + N', @weaver_rows_deleted = @weaver_rows_deleted output'
                    + N', @weaver_rows_rejected = @weaver_rows_rejected output'
                    + N', @weaver_error_message = @weaver_error_message output'
                    + N', @weaver_bookmark_datetime = @weaver_bookmark_datetime output'
                    + N', @weaver_is_static_skip = @weaver_is_static_skip output'
                    + N';';
            end;
        end;

        if @weaver_call is not null and @reload = 1
        begin
            -- End this object's load state before the implementation empties its
            -- target: [_].[LoadStatus] to Pending, and the [_].[Bookmark] row
            -- gone. An absent bookmark row is the one physical shape of "no
            -- clean load has established progress", and it is what the Static
            -- gate below reads. Both are MERGEs, including the deletion: in
            -- every Warehouse but the catalogue's these are views across
            -- databases, and Fabric takes a MERGE through one.
            merge into [_].[LoadStatus] as target
            using (select
                N'Warehouse' as [Item type]
                , @item_name as [Item name]
                , @weaver_schema as [Schema name]
                , @weaver_object as [Object name]
            ) as source
               on
                target.[Item type] = source.[Item type]
                  and target.[Item name] = source.[Item name]
                  and target.[Schema name] = source.[Schema name]
                  and target.[Object name] = source.[Object name]
            when matched then update set
                target.[Workflow ID] = @weaver_workflow
                , target.[Result] = N'Pending'
                , target.[Started datetime] = @weaver_started
                , target.[Completed datetime] = null
                , target.[Duration milliseconds] = null
                , target.[Row update datetime] = sysdatetime()
            when not matched then insert (
                [Item type]
                , [Item name]
                , [Schema name]
                , [Object name]
                , [Workflow ID]
                , [Result]
                , [Started datetime]
                , [Row insert datetime]
                , [Row update datetime]
                , [Row delete datetime]
            )
            values (
                source.[Item type]
                , source.[Item name]
                , source.[Schema name]
                , source.[Object name]
                , @weaver_workflow
                , N'Pending'
                , @weaver_started
                , sysdatetime()
                , sysdatetime()
                , convert(datetime2(6), '9999-12-31 23:59:59.999999')
            );

            merge into [_].[Bookmark] as target
            using (select
                N'Warehouse' as [Item type]
                , @item_name as [Item name]
                , @weaver_schema as [Schema name]
                , @weaver_object as [Object name]
            ) as source
               on
                target.[Item type] = source.[Item type]
                  and target.[Item name] = source.[Item name]
                  and target.[Schema name] = source.[Schema name]
                  and target.[Object name] = source.[Object name]
            when matched then delete;
        end;

        if @weaver_call is not null
        begin
            exec sp_executesql @weaver_call,
                N'@fault_tolerant bit,
                   @ignore_stability_threshold bit,
                   @reload bit,
                   @weaver_succeeded bit output,
                   @weaver_rows_read bigint output,
                   @weaver_rows_inserted bigint output,
                   @weaver_rows_updated bigint output,
                   @weaver_rows_deleted bigint output,
                   @weaver_rows_rejected bigint output,
                   @weaver_error_message varchar(4000) output,
                   @weaver_bookmark_datetime datetime2(6) output,
                   @weaver_is_static_skip bit output',
                @fault_tolerant = @fault_tolerant,
                @ignore_stability_threshold = @ignore_stability_threshold,
                @reload = @reload,
                @weaver_succeeded = @succeeded output,
                @weaver_rows_read = @rows_read output,
                @weaver_rows_inserted = @rows_inserted output,
                @weaver_rows_updated = @rows_updated output,
                @weaver_rows_deleted = @rows_deleted output,
                @weaver_rows_rejected = @rows_rejected output,
                @weaver_error_message = @error_message output,
                @weaver_bookmark_datetime = @bookmark_datetime output,
                @weaver_is_static_skip = @is_static_skip output;
        end;
    end try
    begin catch
        set @weaver_error = error_message();
        set @weaver_error_number = error_number();
    end catch;

    if @weaver_unmatched is not null
    begin
        throw 51030, @weaver_unmatched, 1;
    end;

    set @weaver_completed = sysutcdatetime();
    set @weaver_message = coalesce(@weaver_error, @error_message);
    set @weaver_result =
        case when @weaver_error_number between 51000 and 51999 then N'Failed'
             when @weaver_error is not null then N'Error'
             when @is_static_skip = 1 then N'Skipped'
             when @succeeded = 1 then N'Succeeded'
             when @rows_rejected > 0 then N'Rejected'
             else N'Failed' end;

    merge into [_].[Log] as target
    using (select
        @weaver_log_sk as [Log SK]
    ) as source
       on
        target.[Log SK] = source.[Log SK]
    when not matched then insert (
        [Log SK]
        , [Workflow ID]
        , [Task type]
        , [Target type]
        , [Target name]
        , [Schema name]
        , [Object name]
        , [Result]
        , [Started datetime]
        , [Completed datetime]
        , [Duration milliseconds]
        , [Message]
        , [Details]
        , [Row insert datetime]
        , [Row update datetime]
        , [Row delete datetime]
    )
    values (
        source.[Log SK]
        , @weaver_workflow
        , N'load'
        , N'Warehouse'
        , db_name()
        , @weaver_schema
        , @weaver_object
        , @weaver_result
        , @weaver_started
        , @weaver_completed
        , datediff(millisecond, @weaver_started, @weaver_completed)
        , @weaver_message
        , null
        , sysdatetime()
        , sysdatetime()
        , convert(datetime2(6), '9999-12-31 23:59:59.999999')
    );

    merge into [_].[LoadStatus] as target
    using (select
        N'Warehouse' as [Item type]
        , @item_name as [Item name]
        , @weaver_schema as [Schema name]
        , @weaver_object as [Object name]
    ) as source
       on
        target.[Item type] = source.[Item type]
          and target.[Item name] = source.[Item name]
          and target.[Schema name] = source.[Schema name]
          and target.[Object name] = source.[Object name]
    when matched then update set
        target.[Workflow ID] = @weaver_workflow
        , target.[Result] = @weaver_result
        , target.[Started datetime] = @weaver_started
        , target.[Completed datetime] = @weaver_completed
        , target.[Duration milliseconds] = datediff(millisecond, @weaver_started, @weaver_completed)
        , target.[Row update datetime] = sysdatetime()
    when not matched then insert (
        [Item type]
        , [Item name]
        , [Schema name]
        , [Object name]
        , [Workflow ID]
        , [Result]
        , [Started datetime]
        , [Completed datetime]
        , [Duration milliseconds]
        , [Row insert datetime]
        , [Row update datetime]
        , [Row delete datetime]
    )
    values (
        source.[Item type]
        , source.[Item name]
        , source.[Schema name]
        , source.[Object name]
        , @weaver_workflow
        , @weaver_result
        , @weaver_started
        , @weaver_completed
        , datediff(millisecond, @weaver_started, @weaver_completed)
        , sysdatetime()
        , sysdatetime()
        , convert(datetime2(6), '9999-12-31 23:59:59.999999')
    );

    merge into [_].[LoadStatistic] as target
    using (select
        @weaver_statistic_sk as [Load statistic SK]
    ) as source
       on
        target.[Load statistic SK] = source.[Load statistic SK]
    when not matched then insert (
        [Load statistic SK]
        , [Workflow ID]
        , [Item type]
        , [Item name]
        , [Schema name]
        , [Object name]
        , [Started datetime]
        , [Completed datetime]
        , [Duration milliseconds]
        , [Rows read]
        , [Rows inserted]
        , [Rows updated]
        , [Rows deleted]
        , [Rows rejected]
        , [Is reload]
        , [Is static skip]
        , [Row insert datetime]
        , [Row update datetime]
        , [Row delete datetime]
    )
    values (
        source.[Load statistic SK]
        , @weaver_workflow
        , N'Warehouse'
        , @item_name
        , @weaver_schema
        , @weaver_object
        , @weaver_started
        , @weaver_completed
        , datediff(millisecond, @weaver_started, @weaver_completed)
        , coalesce(@rows_read, 0)
        , coalesce(@rows_inserted, 0)
        , coalesce(@rows_updated, 0)
        , coalesce(@rows_deleted, 0)
        , coalesce(@rows_rejected, 0)
        , cast(@reload as bit)
        , cast(coalesce(@is_static_skip, 0) as bit)
        , sysdatetime()
        , sysdatetime()
        , convert(datetime2(6), '9999-12-31 23:59:59.999999')
    );

    if @weaver_result = N'Succeeded' and @bookmark_datetime is not null
    begin
        merge into [_].[Bookmark] as target
        using (select
            N'Warehouse' as [Item type]
            , @item_name as [Item name]
            , @weaver_schema as [Schema name]
            , @weaver_object as [Object name]
        ) as source
           on
            target.[Item type] = source.[Item type]
              and target.[Item name] = source.[Item name]
              and target.[Schema name] = source.[Schema name]
              and target.[Object name] = source.[Object name]
        when matched then update set
            target.[Bookmark datetime] = @bookmark_datetime
            , target.[Row update datetime] = sysdatetime()
        when not matched then insert (
            [Item type]
            , [Item name]
            , [Schema name]
            , [Object name]
            , [Bookmark datetime]
            , [Row insert datetime]
            , [Row update datetime]
            , [Row delete datetime]
        )
        values (
            source.[Item type]
            , source.[Item name]
            , source.[Schema name]
            , source.[Object name]
            , @bookmark_datetime
            , sysdatetime()
            , sysdatetime()
            , convert(datetime2(6), '9999-12-31 23:59:59.999999')
        );
    end;

    if @weaver_error is not null
    begin
        set @weaver_rethrow = cast(@weaver_error as nvarchar(2048));
        declare @weaver_number int =
            case when @weaver_error_number >= 50000 then @weaver_error_number
                 else 51031 end;
        throw @weaver_number, @weaver_rethrow, 1;
    end;
end;
