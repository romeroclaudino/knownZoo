create schema if not exists knownzoo;

use knownzoo;

create table if not exists recinto(
	Id int auto_increment not null,
    Nome varchar(120) not null,
    AnimalId int,
    primary key (Id)
);

create table if not exists animal (
	Id int auto_increment not null,
	Raca varchar(120) not null,
	primary key (Id)
);

create table if not exists dados (
	Id int auto_increment not null,
    Mac varchar(45) not null,
    MomentoEntrada datetime,
    MomentoSaida datetime,
    Permanencia double,
	RecintoId int,
    primary key (Id)
);

alter table recinto add foreign key (AnimalId) references animal(Id);
alter table dados add foreign key (RecintoId) references recinto(Id); 