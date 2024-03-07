create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
AS
set nocount on
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)--1)после оператора DECLARE название должны начинаться с новой строки
	declare @ErrorMessage varchar(max) --2) не рекомендуется использовать max

-- Проверка на корректность загрузки
	if not exists (
	select 1 --3)SELECT должен находиться под exists
	from syn.ImportFile as f --4)неправильно определен алиас
	where f.ID = @ID_Record--5)весь этот блок должен идти с дополнительным отступом
		and f.FlagLoaded = cast(1 as bit)
	)
		BEGIN --6)BEGIN должен находится на одном уровне с if
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'
			raiserror(@ErrorMessage, 3, 1)  -- 7)необходимо поставить вместо 1 0 поскольку это предупреждение, а не ошибка
			return
		end--8)аналогично END должен находиться под IF и begin

	-- Чтение из слоя временных данных 9)не написано за какой период прочитаны данные)
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType--10)нет дополнительного отступа при множественном перечислении
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive  --11)нет пробела в выражении IS NULL. во всем блоке пропущен дополнительный пробел
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and c_dist.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not NULL --12)нет пробела в IS NULL 

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной 13) неправильно оформлен многострочный комментарий
	select
		cs.*
		,case
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"' --14)THEN должен быть на новой строке
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where c.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	merge into syn.CustomerSeasonal as cs --15)INTO не используется
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched--16)пропущен оператор then
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then--17)таблица t не определена
		update
		set ID_CustomerSystemType = s.ID_CustomerSystemType -- 18)условие для SET должно находится на новой строке
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		raiserror(@ErrorMessage, 1, 1)

		--Формирование таблицы для отчетности (19)не указан период за который проводится отчетность
		select top 100
			bir.Season as 'Сезон'
			,bir.UID_DS_Customer as 'UID Клиента'
			,bir.Customer as 'Клиент'
			,bir.CustomerSystemType as 'Тип клиента'
			,bir.UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,bir.CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(bir.DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateBegin) as 'Дата начала'
			,isnull(format(try_cast(birDateEnd as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateEnd) as 'Дата окончания'
			,bir.FlagActive as 'Активность'
			,bir.Reason as 'Причина'
		from #BadInsertedRows as bir --20)Неправильно определен алиас

		return
	end
end
