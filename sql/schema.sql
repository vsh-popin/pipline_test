CREATE TABLE IF NOT EXISTS departments (
  id SERIAL PRIMARY KEY,
  name varchar(32) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS sensors (
  id SERIAL PRIMARY KEY,
  serial varchar(64) UNIQUE NOT NULL,
  department_id int NOT NULL REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  name varchar(16) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_logs (
  id BIGSERIAL PRIMARY KEY,
  sensor_id int NOT NULL REFERENCES sensors(id),
  product_id int REFERENCES products(id),
  create_at timestamp NOT NULL,
  product_expire timestamp NOT NULL
);


CREATE UNIQUE INDEX IF NOT EXISTS unique_sensor_logs_triple
  ON sensor_logs(sensor_id, product_id, create_at);