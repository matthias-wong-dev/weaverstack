create or alter procedure [_].[Test]
    @object_name varchar(261)
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
    declare @weaver_test_type varchar(128) = null;
    declare @weaver_failure_count bigint = null;
    declare @missing_count bigint = null;
    declare @unexpected_count bigint = null;
    declare @violation_count bigint = null;

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
            declare @weaver_test_procedure nvarchar(261) = N'[_].[' + replace(
                N'Test ' + @object_name, N']', N']]') + N']';
            declare @weaver_assumption_procedure nvarchar(261) = N'[_].[' + replace(
                N'Assumption ' + @object_name, N']', N']]') + N']';
            declare @weaver_has_test int =
                case when object_id(@weaver_test_procedure, 'P') is null then 0 else 1 end;
            declare @weaver_has_assumption int =
                case when object_id(@weaver_assumption_procedure, 'P') is null then 0 else 1 end;
            if @weaver_has_test = 1 and @weaver_has_assumption = 1
            begin
                set @weaver_unmatched = concat(@object_name,
                    N' is installed as both a Test and an Assumption; repair the installation');
            end
            else if @weaver_has_test = 0 and @weaver_has_assumption = 0
            begin
                set @weaver_unmatched = concat(@object_name,
                    N' is not a validation in this Warehouse');
            end
            else if @weaver_has_test = 1
            begin
                set @weaver_test_type = N'Test';
                set @weaver_target = @weaver_test_procedure;
                set @weaver_call = N'exec ' + @weaver_target + N' '
                    + N'@missing_count = @missing_count output'
                    + N', @unexpected_count = @unexpected_count output'
                    + N';';
            end
            else
            begin
                set @weaver_test_type = N'Assumption';
                set @weaver_target = @weaver_assumption_procedure;
                set @weaver_call = N'exec ' + @weaver_target + N' '
                    + N'@violation_count = @violation_count output'
                    + N';';
            end;
        end;

        if @weaver_call is not null
        begin
            exec sp_executesql @weaver_call,
                N'@missing_count bigint output,
                   @unexpected_count bigint output,
                   @violation_count bigint output',
                @missing_count = @missing_count output,
                @unexpected_count = @unexpected_count output,
                @violation_count = @violation_count output;
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
    set @weaver_message = @weaver_error;
    set @weaver_failure_count =
        case when @weaver_error is not null then null
             else coalesce(@missing_count, 0) + coalesce(@unexpected_count, 0)
                  + coalesce(@violation_count, 0) end;
    set @weaver_result =
        case when @weaver_error is not null then N'Error'
             when @weaver_failure_count > 0 then N'Failed'
             else N'Succeeded' end;

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
        , N'test'
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

    merge into [_].[TestStatus] as target
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
        target.[Test type] = @weaver_test_type
        , target.[Workflow ID] = @weaver_workflow
        , target.[Result] = @weaver_result
        , target.[Started datetime] = @weaver_started
        , target.[Completed datetime] = @weaver_completed
        , target.[Duration milliseconds] = datediff(millisecond, @weaver_started, @weaver_completed)
        , target.[Failure count] = @weaver_failure_count
        , target.[Row update datetime] = sysdatetime()
    when not matched then insert (
        [Item type]
        , [Item name]
        , [Schema name]
        , [Object name]
        , [Test type]
        , [Workflow ID]
        , [Result]
        , [Started datetime]
        , [Completed datetime]
        , [Duration milliseconds]
        , [Failure count]
        , [Row insert datetime]
        , [Row update datetime]
        , [Row delete datetime]
    )
    values (
        source.[Item type]
        , source.[Item name]
        , source.[Schema name]
        , source.[Object name]
        , @weaver_test_type
        , @weaver_workflow
        , @weaver_result
        , @weaver_started
        , @weaver_completed
        , datediff(millisecond, @weaver_started, @weaver_completed)
        , @weaver_failure_count
        , sysdatetime()
        , sysdatetime()
        , convert(datetime2(6), '9999-12-31 23:59:59.999999')
    );

    if @weaver_error is not null
    begin
        set @weaver_rethrow = cast(@weaver_error as nvarchar(2048));
        declare @weaver_number int =
            case when @weaver_error_number >= 50000 then @weaver_error_number
                 else 51031 end;
        throw @weaver_number, @weaver_rethrow, 1;
    end;
end;
