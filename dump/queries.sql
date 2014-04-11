select distinct histclass1, histclass2, histclass3, year from russianrepository where histclass1='Водные сообщения' and year='1897';

# Query(a)
select distinct histclass1, value_unit, histclass2, histclass3, year, value from russianrepository where histclass1='Водные сообщения' and histclass2='Коммерческое судоходство' and histclass3='мужчины' and year='1897' and territory='Акмолинская' and value_unit='человек' limit 10;

# Query(b)
select distinct histclass1, value_unit, histclass2, histclass3, year, value from russianrepository where territory='Акмолинская' limit 10;
select distinct histclass1 from russianrepository where territory='Акмолинская' and histclass1='Обработка растительных и животных питательных продуктов' and year>='1897' limit 10
