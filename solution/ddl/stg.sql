drop table if exists dds.h_user;

create table dds.h_user (
  h_user_pk uuid PRIMARY KEY,
  user_id varchar not null,
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.h_product;

create table dds.h_product (
  h_product_pk uuid PRIMARY KEY,
  product_id varchar not null,
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.h_category;

create table dds.h_category (
  h_category_pk uuid PRIMARY KEY,
  category_name varchar not null,
  load_dt timestamp not null,
  load_src varchar not null
);


drop table if exists dds.h_restaurant;

create table dds.h_restaurant (
  h_restaurant_pk uuid PRIMARY KEY,
  restaurant_id varchar not null,
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.h_order;

create table dds.h_order (
  h_order_pk uuid PRIMARY KEY,
  order_id int not null,
  order_dt timestamp not null,
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.l_order_product;

create table dds.l_order_product (
  hk_order_product_pk uuid PRIMARY KEY,
  h_order_pk uuid not null  references dds.h_order(h_order_pk),
  h_product_pk uuid not null references dds.h_product(h_product_pk),
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.l_product_restaurant;

create table dds.l_product_restaurant (
  hk_product_restaurant_pk uuid PRIMARY KEY,
  h_product_pk uuid not null  references dds.h_product(h_product_pk),
  h_restaurant_pk uuid not null references dds.h_restaurant(h_restaurant_pk),
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.l_product_category;

create table dds.l_product_category (
  hk_product_category_pk uuid PRIMARY KEY,
  h_product_pk uuid not null  references dds.h_product(h_product_pk),
  h_category_pk uuid not null references dds.h_category(h_category_pk),
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.l_order_user;

create table dds.l_order_user (
  hk_order_user_pk uuid PRIMARY KEY,
  h_order_pk uuid not null  references dds.h_order(h_order_pk),
  h_user_pk uuid not null references dds.h_user(h_user_pk),
  load_dt timestamp not null,
  load_src varchar not null
);

drop table if exists dds.s_user_names;

create table dds.s_user_names (
  h_user_pk uuid not null references dds.h_user(h_user_pk),
  username varchar not null,
  userlogin varchar not null,
  load_dt timestamp not null,
  load_src varchar not null,
  hk_user_names_hashdiff uuid not null,
  PRIMARY KEY (h_user_pk, load_dt),
  constraint s_user_names_unq unique (hk_user_names_hashdiff)
);

drop table if exists dds.s_product_names;

create table dds.s_product_names (
  h_product_pk uuid not null references dds.h_product(h_product_pk),
  name varchar not null,
  load_dt timestamp not null,
  load_src varchar not null,
  hk_product_names_hashdiff uuid not null,
  PRIMARY KEY (h_product_pk, load_dt),
  constraint s_product_names_unq unique (hk_product_names_hashdiff)
);

drop table if exists dds.s_restaurant_names;

create table dds.s_restaurant_names (
  h_restaurant_pk uuid not null references dds.h_restaurant(h_restaurant_pk),
  name varchar not null,
  load_dt timestamp not null,
  load_src varchar not null,
  hk_restaurant_names_hashdiff uuid not null,
  PRIMARY KEY (h_restaurant_pk, load_dt),
  constraint s_restaurant_names_unq unique (hk_restaurant_names_hashdiff)
);

drop table if exists dds.s_order_cost;

create table dds.s_order_cost (
  h_order_pk uuid not null references dds.h_order(h_order_pk),
  cost decimal(19, 5) not null,
  payment decimal(19, 5) not null,
  load_dt timestamp not null,
  load_src varchar not null,
  hk_order_cost_hashdiff uuid not null,
  PRIMARY KEY (h_order_pk, load_dt),
  constraint s_order_cost_unq unique (hk_order_cost_hashdiff)
);

drop table if exists dds.s_order_status;

create table dds.s_order_status (
  h_order_pk uuid not null references dds.h_order(h_order_pk),
  status varchar not null,
  load_dt timestamp not null,
  load_src varchar not null,
  hk_order_status_hashdiff uuid not null,
  PRIMARY KEY (h_order_pk, load_dt),
  constraint s_order_status_unq unique (hk_order_status_hashdiff)
);