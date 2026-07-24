set nocount on;

if schema_id(N'TestA') is null
begin
    exec(N'create schema TestA');
end;

if schema_id(N'TestB') is null
begin
    exec(N'create schema TestB');
end;

create table TestA.Parent
(
    ParentId int not null
  , Name     varchar(100) not null
);

create table TestA.Child
(
    ChildId  int not null
  , ParentId int not null
);

create table TestB.Independent
(
    IndependentId int not null
  , Description   varchar(100) null
);

insert into TestA.Parent (ParentId, Name)
values (1, 'one');

insert into TestA.Child (ChildId, ParentId)
values (10, 1);

insert into TestB.Independent (IndependentId, Description)
values (100, 'independent');

exec(N'
create view TestA.ParentView
as
select ParentId, Name
from TestA.Parent;
');

exec(N'
create view TestB.CrossSchemaView
as
select
    child.ChildId
  , parent.Name
from TestA.Child as child
join TestA.ParentView as parent on parent.ParentId = child.ParentId;
');

exec(N'
create procedure TestA.RefreshParent
as
begin
    set nocount on;
    select ParentId, Name from TestA.Parent;
end;
');
