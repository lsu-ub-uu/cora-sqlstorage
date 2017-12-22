create table trying(
id VARCHAR(30),
stuff VARCHAR(30),
	constraint PK_USER_ID primary key(ID)
);
insert into trying (id, stuff) values ('1','some stuff');

create table records (
	recordType varchar(500),
	recordId varchar(500),
	dataDivider varchar(100),
	data json,
	constraint pk_records primary key(recordType, recordId)
);
