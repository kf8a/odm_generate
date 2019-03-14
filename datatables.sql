create sequence sites_site_code_seq;

create sequence variables_variable_code_seq;

create sequence methods_method_code_seq;

create table sites
(
  site_code           serial not null,
  site_name           text,
  latitude            double precision,
  longitude           double precision,
  latlongdatumsrsname text,
  site_type           text,
  comments            text
);

create table sources
(
  source_code        text,
  organization       text,
  source_description text,
  source_link        text,
  contact_name       text,
  email              text,
  citation           text
);

create table variables
(
  variable_name      text
    constraint variables_variable_name_pk
    unique,
  variable_unit_name text,
  data_type          text,
  sample_medium      text,
  value_type         text,
  isregular          boolean,
  time_support       real,
  time_unit_name     text,
  general_category   text,
  no_data_value      text,
  variable_code      serial not null
    constraint variables_variable_code_pk
    primary key
);

create unique index variables_variable_code_uindex
  on variables (variable_code);

create table mapping
(
  variable_name text not null,
  variable_code integer,
  method_code   integer,
  source_code   integer,
  table_name    text
);

create unique index mapping_variable_name_uindex
  on mapping (variable_name);

create table methods
(
  method_code        serial not null
    constraint methods_pkey
    primary key,
  method_description text,
  method_link        text
);

