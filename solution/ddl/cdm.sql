drop table cdm.user_product_counters;

create table cdm.user_product_counters ( 
  id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS identity,
  user_id uuid not null,
  product_id uuid not null,
  product_name varchar not null,
  order_cnt integer not null constraint user_product_counters_order_cnt check (order_cnt >= 0)
);

alter table cdm.user_product_counters add constraint user_product_counters_unique_user_product UNIQUE (user_id, product_id);
 

drop table cdm.user_category_counters;
 
create table cdm.user_category_counters ( 
  id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS identity,
  user_id uuid not null,
  category_id uuid not null,
  category_name varchar not null,
  order_cnt integer not null constraint user_category_counters_order_cnt check (order_cnt >= 0)
);

alter table cdm.user_category_counters add constraint user_category_counters_unique_user_category UNIQUE (user_id, category_id);